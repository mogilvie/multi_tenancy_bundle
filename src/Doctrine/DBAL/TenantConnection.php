<?php

namespace Hakam\MultiTenancyBundle\Doctrine\DBAL;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Events;
use Doctrine\DBAL\Event;
use Doctrine\DBAL\Exception;

/**
 * @author Ramy Hakam <pencilsoft1@gmail.com>
 */
class TenantConnection extends Connection
{
    /** @var bool */
    protected bool $isConnected = false;
    /** @var bool */
    protected bool $autoCommit = true;

    /**
     * @return bool
     * @throws Exception
     */
    public function switchConnection(array $params): bool
    {
        if ($this->isConnected()) {
            $this->close();
        }

        $existingParams = $this->getParams();
        $params = array_merge($existingParams, $newParams);

        $connection = parent::__construct($params, $this->_driver, $this->_config, $this->_eventManager);
        
        if ($this->autoCommit === false) {
            $this->beginTransaction();
        }
        if ($this->_eventManager->hasListeners(Events::postConnect)) {
            $eventArgs = new Event\ConnectionEventArgs($this);
            $this->_eventManager->dispatchEvent(Events::postConnect, $eventArgs);
        }
        return true;
    }

    public function close(): void
    {
        $this->_conn = null;
        $this->isConnected = false;
    }
}
