<?php

namespace Hakam\MultiTenancyBundle\Services;

use Doctrine\Common\Annotations\AnnotationReader;
use Doctrine\Common\Annotations\AnnotationRegistry;
use Doctrine\Common\Cache\Psr6\DoctrineProvider;
use Doctrine\ORM\Configuration;
use Doctrine\DBAL\Driver\AbstractMySQLDriver;
use Doctrine\DBAL\Driver\AbstractPostgreSQLDriver;
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Exception;
use Doctrine\DBAL\Tools\DsnParser;
use Doctrine\ORM\EntityManager;
use Doctrine\ORM\EntityManagerInterface;
use Doctrine\ORM\Mapping\Driver\AnnotationDriver;
use Doctrine\ORM\Tools\SchemaTool;
use Doctrine\ORM\Tools\Setup;
use Hakam\MultiTenancyBundle\Doctrine\ORM\TenantEntityManager;
use Hakam\MultiTenancyBundle\Enum\DatabaseStatusEnum;
use Hakam\MultiTenancyBundle\Event\SwitchDbEvent;
use Hakam\MultiTenancyBundle\Exception\MultiTenancyException;
use Psr\EventDispatcher\EventDispatcherInterface;
use Symfony\Component\Cache\Psr16Cache;
use Symfony\Component\DependencyInjection\Attribute\Autowire;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\Cache\Adapter\ArrayAdapter;

/**
 * @author Mark Ogilvie <m.ogilvie@parolla.ie>
 */
class DbService
{
    public function __construct(
        private EventDispatcherInterface $eventDispatcher,
        private TenantEntityManager      $tenantEntityManager,
        private EntityManagerInterface   $entityManager,
        #[Autowire('%hakam.tenant_db_list_entity%')]
        private string                   $tenantDbListEntity,
        #[Autowire('%hakam.tenant_db_credentials%')]
        private array                    $dbCredentials
    )
    {
    }

    public function createDatabase(string $dbName): int
    {
        // Central management connection credentials (e.g., 'mysql' database for MySQL)
        $managementDbCredentials = $this->dbCredentials;
        $managementDbCredentials['dbname'] = 'parolla_plugins'; // or the appropriate central database name

        // Establish a connection to the central management database
        $dsnParser = new DsnParser(['mysql' => 'pdo_mysql']);
        $managementConnection = DriverManager::getConnection($dsnParser->parse($managementDbCredentials['db_url']));

        try {
            $platform = $managementConnection->getDatabasePlatform();
            if ($managementConnection->getDriver() instanceof AbstractMySQLDriver || $managementConnection->getDriver() instanceof AbstractPostgreSQLDriver) {
                $sql = $platform->getListDatabasesSQL();
            } else {
                // Support SQLite and other databases
                $sql = 'SELECT name FROM sqlite_master WHERE type = "database"';
            }
            $statement = $managementConnection->executeQuery($sql);
            $databaseList = $statement->fetchFirstColumn();

            if (in_array($dbName, $databaseList)) {
                throw new MultiTenancyException(sprintf('Database %s already exists.', $dbName), Response::HTTP_BAD_REQUEST);
            }

            // Create the new tenant database
            $schemaManager = method_exists($managementConnection, 'createSchemaManager')
                ? $managementConnection->createSchemaManager()
                : $managementConnection->getSchemaManager();
            $schemaManager->createDatabase($dbName);
            return 1;
        } catch (\Exception $e) {
            throw new MultiTenancyException(sprintf('Unable to create new tenant database %s: %s', $dbName, $e->getMessage()), $e->getCode(), $e);
        } finally {
            $managementConnection->close();
        }
    }

    public function createSchemaInDb(string $dbName): void
    {
        $entityManager = $this->tenantEntityManager;

        $tenantConfiguration = $entityManager->getConfiguration();
        // Central management connection credentials (e.g., 'mysql' database for MySQL)
        $managementDbCredentials = $this->dbCredentials;
        $managementDbCredentials['dbname'] = 'mysql'; // or the appropriate central database name

        // Establish a connection to the central management database
        $dsnParser = new DsnParser(['mysql' => 'pdo_mysql']);
        $managementConnection = DriverManager::getConnection($dsnParser->parse($managementDbCredentials['db_url']));

        try {
            // Check if the tenant database exists
            $platform = $managementConnection->getDatabasePlatform();
            $sql = $platform->getListDatabasesSQL();
            $statement = $managementConnection->executeQuery($sql);
            $databaseList = $statement->fetchFirstColumn();

            if (!in_array($dbName, $databaseList)) {
                throw new MultiTenancyException(sprintf('Database %s does not exist.', $dbName), Response::HTTP_BAD_REQUEST);
            }

            // Parse DSN to connect to the tenant database
            $tenantDbCredentials = $managementConnection->getParams();
            $tenantDbCredentials['dbname'] = $dbName;
            $tenantConnection = DriverManager::getConnection($tenantDbCredentials);

            // Ensure the database is selected
            $tenantConnection->exec(sprintf('USE `%s`', $dbName));

            // Create the new EntityManager for the tenant database
            $tenantEntityManager = EntityManager::create($tenantConnection, $tenantConfiguration);

            // Begin the transaction
            $tenantConnection->beginTransaction();
            $tenantConnection->setAutoCommit(false);

            try {
                // Get all entity classes
                $metadataFactory = $tenantEntityManager->getMetadataFactory();
                $allMetadata = $metadataFactory->getAllMetadata();

                // Generate the SQL statements for updating the schema
                $schemaTool = new SchemaTool($tenantEntityManager);
                $updateSql = $schemaTool->getUpdateSchemaSql($allMetadata, true);

                // Check if there are any SQL statements to execute
                if (!empty($updateSql)) {
                    // Execute each SQL statement
                    foreach ($updateSql as $sql) {
                        $tenantConnection->executeStatement($sql);
                    }
                }
                // Commit the transaction
                $tenantConnection->commit();
            } catch (\Exception $e) {
                // Ensure the connection is active before rolling back
                $tenantConnection->rollBack();
                throw new MultiTenancyException(sprintf('Failed to update schema for database %s: %s', $dbName, $e->getMessage()), $e->getCode(), $e);
            } finally {
                // Ensure the connection is closed
                $tenantEntityManager->close();
                $tenantConnection->close();
            }
        } catch (\Exception $e) {
            throw new MultiTenancyException(sprintf('Unable to check or update schema for database %s: %s', $dbName, $e->getMessage()), $e->getCode(), $e);
        } finally {
            // Ensure the management connection is closed
            $managementConnection->close();
        }
    }

    /**
     * Creates a schema in the specified tenant database.
     *
     * @param int $UserDbId The tenant database ID.
     */
    public function createSchemaInDBOld(int $UserDbId): void
    {
        $this->eventDispatcher->dispatch(new SwitchDbEvent($UserDbId));

        $entityManager = $this->tenantEntityManager;

        try {
            $entityManager->beginTransaction(); // Begin the transaction

            $schemaTool = new SchemaTool($entityManager);

            $metadata = $entityManager->getMetadataFactory()->getAllMetadata();

            $sqls = $schemaTool->getUpdateSchemaSql($metadata, true);

            if (empty($sqls)) {
                return;
            }

            $schemaTool->updateSchema($metadata);

            $entityManager->commit(); // Commit the transaction
        } catch (\PDOException $e) {

        } catch (\Exception $e) {
            $entityManager->rollback(); // Rollback the transaction on error
            throw $e; // Rethrow the exception after rollback
        } finally {
            $entityManager->close(); // Close the entity manager
        }
    }

    /**
     * Drops the specified database.
     *
     * @param string $dbName The name of the database to drop.
     * @throws MultiTenancyException|Exception If the database does not exist or cannot be dropped.
     */
    public function dropDatabase(string $dbName): void
    {
        $connection = $this->tenantEntityManager->getConnection();

        $params = $connection->getParams();

        $tmpConnection = DriverManager::getConnection($params);

        $schemaManager = method_exists($tmpConnection, 'createSchemaManager')
            ? $tmpConnection->createSchemaManager()
            : $tmpConnection->getSchemaManager();

        $shouldNotCreateDatabase = !in_array($dbName, $schemaManager->listDatabases());

        if ($shouldNotCreateDatabase) {
            throw new MultiTenancyException(sprintf('Database %s does not exist.', $dbName), Response::HTTP_BAD_REQUEST);
        }

        try {
            $schemaManager->dropDatabase($dbName);
        } catch (\Exception $e) {
            throw new MultiTenancyException(sprintf('Unable to create new tenant database %s: %s', $dbName, $e->getMessage()), $e->getCode(), $e);
        }

        $tmpConnection->close();
    }

    private function onboardNewDatabaseConfig(string $dbname): int
    {
        //check if db already exists
        $dbConfig = $this->entityManager->getRepository($this->tenantDbListEntity)->findOneBy(['dbName' => $dbname]);
        if ($dbConfig) {
            return $dbConfig->getId();
        }
        $newDbConfig = new   $this->tenantDbListEntity();
        $newDbConfig->setDbName($dbname);
        $this->entityManager->persist($newDbConfig);
        $this->entityManager->flush();
        return $newDbConfig->getId();
    }

    public function getListOfNotCreatedDataBases(): array
    {
        return $this->entityManager->getRepository($this->tenantDbListEntity)->findBy(['databaseStatus' => DatabaseStatusEnum::DATABASE_NOT_CREATED]);
    }

    public function getDefaultTenantDataBase(): TenantDbConfigurationInterface
    {
        return $this->entityManager->getRepository($this->tenantDbListEntity)->findOneBy(['databaseStatus' => DatabaseStatusEnum::DATABASE_CREATED]);
    }
}
