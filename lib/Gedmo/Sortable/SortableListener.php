<?php

namespace Gedmo\Sortable;

use Doctrine\Common\EventArgs;
use Gedmo\Mapping\MappedEventSubscriber;
use Gedmo\Sluggable\Mapping\Event\SortableAdapter;
use Doctrine\ORM\Proxy\Proxy;

/**
 * The SortableListener maintains a sort index on your entities
 * to enable arbitrary sorting.
 *
 * This behavior can inpact the performance of your application
 * since it does some additional calculations on persisted objects.
 *
 * @author Lukas Botsch <lukas.botsch@gmail.com>
 * @author Julien Pottier <julien.pottier@isics.fr>
 * @author Sébastien Cas <sebastien.cas@isics.fr>
 * @subpackage SortableListener
 * @package Gedmo.Sortable
 * @link http://www.gediminasm.org
 * @license MIT License (http://www.opensource.org/licenses/mit-license.php)
 */
class SortableListener extends MappedEventSubscriber
{
    const TO_DELETE = 1;
    const TO_INSERT = 2;
    const TO_UPDATE = 3;
    
    /**
     * @var object $em ObjectManager
     */
    private $objectManager;
    
    /**
     * @var array $configs Configuration by meta name
     */
    private $configs = array();
    
    /**
     * @var array $maxPositions Max positions by hash (groups)
     */
    private $maxPositions = array();
    
    /**
     * @var array $toProcess Informations to process
     */
    private $toProcess = array();
    
    
    
    /**
     * Specifies the list of events to listen
     *
     * @return array
     */
    public function getSubscribedEvents()
    {
        return array('onFlush', 'loadClassMetadata');
    }

    /**
     * Mapps additional metadata
     *
     * @param EventArgs $eventArgs
     * @return void
     */
    public function loadClassMetadata(EventArgs $args)
    {
        $ea = $this->getEventAdapter($args);
        $this->loadMetadataForObjectClass($ea->getObjectManager(), $args->getClassMetadata());
    }

    /**
     * Generate slug on objects being updated during flush if they require changing
     *
     * @param EventArgs $args
     */
    public function onFlush(EventArgs $args)
    {
        $this->objectManager = $this->getEventAdapter($args)->getObjectManager();
        $uow = $this->objectManager->getUnitOfWork();
        
        // Add objects beeing deleted to the process list
        foreach ($uow->getScheduledEntityDeletions() as $object) {
            $this->addObjectToProcess($object, self::TO_DELETE);
        }
        // Add objects beeing inserted to the process list
        foreach ($uow->getScheduledEntityInsertions() as $object) {
            $this->addObjectToProcess($object, self::TO_INSERT);
        }
        // Add objects beeing updated to the process list
        foreach ($uow->getScheduledEntityUpdates() as $object) {
            $this->addObjectToProcess($object, self::TO_UPDATE);
        }

        // Process
        foreach ($this->toProcess as &$info) {
            switch ($info['operation']) {
                case self::TO_DELETE:
                    $this->processDeletion($info);
                    break;
                case self::TO_INSERT:
                    $this->processInsertion($info);
                    break;
            }
        }
        
        // Recompute entity changeset for all scheduled entity
        foreach ($this->toProcess as &$info) {
            if ($info['operation'] == self::TO_INSERT) {
                $uow->recomputeSingleEntityChangeSet($info['meta'], $info['object']);
            }
        }
       
        $this->toProcess = array();
        $this->maxPositions = array();
        $this->configs = array();
    }
    
    /**
     * Add object to the list of objects to process 
     *
     * @param object $object Object
     * @param string $operation Operation
     */
    private function addObjectToProcess($object, $operation)
    {
        $meta = $this->objectManager->getClassMetadata(get_class($object));
        
        if (!isset($this->configs[$meta->name])) {
            $config = $this->getConfiguration($this->objectManager, $meta->name);
            if (!empty($config)) {
               $this->configs[$meta->name] = $config;  
            }
        }
        
        if (isset($this->configs[$meta->name])) {
            $config = $this->configs[$meta->name];
            $groups = $this->getGroups($object, $meta);
            $hash = $this->getHash($groups, $config['useObjectClass']);
            $identifier = spl_object_hash($object);
            
            // Object informations
            $info = array('meta' => $meta, 'object' => $object, 'groups' => $groups, 'hash' => $hash, 'identifier' => $identifier, 'operation' => $operation);
            
            // An update = a deletion + an insertion
            if ($operation === self::TO_UPDATE) {
                // Get database groups and position of object
                // and create an object with those values (object to delete)
                $uow = $this->objectManager->getUnitOfWork();
                $changeSet = $uow->getEntityChangeSet($object);
                
                // Restore old groups
                $hasChanged = false;
                $oldObject = clone $object;
                if (isset($config['groups'])) {
                    foreach ($groups as $key => $group) {
                        if (array_key_exists($key, $changeSet) && $changeSet[$key][0] != $changeSet[$key][1]) {
                            $meta->getReflectionProperty($key)->setValue($oldObject, $changeSet[$key][0]);
                            $hasChanged = true;
                        }
                    }
                }
                                
                // Restore old position
                if (array_key_exists($config['position'], $changeSet)) {
                    $oldPosition = $changeSet[$config['position']][0];
                    $meta->getReflectionProperty($config['position'])->setValue($oldObject, $oldPosition);
                    if ($changeSet[$config['position']][0] != $changeSet[$config['position']][1]) {
                        $hasChanged = true;
                    }
                }
                
                if ($hasChanged) {
                    // Old groups
                    $oldGroups = $this->getGroups($oldObject, $meta);
                    
                    // Old hash
                    $oldHash = $this->getHash($oldGroups, $config['useObjectClass']);

                    // Old info use to delete "old" object
                    $oldInfo = $info;
                    $oldInfo['object'] = $oldObject;
                    $oldInfo['groups'] = $oldGroups;
                    $oldInfo['hash'] = $oldHash;
                    $oldInfo['operation'] = self::TO_DELETE;
                    $this->toProcess[] = $oldInfo;
                    
                    // Add "new" object to process liste for insertion
                    $info['operation'] = self::TO_INSERT;
                }
            }

            $this->toProcess[] = $info;
        }
    }
    
    /**
     * Returns hash
     *
     * @param array $groups Groups
     * @param string $useObjectClass Object class
     *
     * @return string
     */
    private function getHash(array $groups, $useObjectClass)
    {
        $data = $useObjectClass;
        foreach ($groups as $group => $val) {
            if (is_object($val)) {
                $val = spl_object_hash($val);
            }
            $data .= $group.$val;
        }

        return md5($data);
    }

    /**
     * Returns object groups
     *
     * @param object $object Object
     * @param ClassMetadata $meta Class meta data
     *
     * @return array
     */
    private function getGroups($object, $meta = null)
    {
        if (null === $meta) {
            $meta = $this->objectManager->getClassMetadata(get_class($object));
        }
        
        $groups = array();
        $config = $this->configs[$meta->name];
        if (isset($config['groups'])) {
            foreach ($config['groups'] as $group) {
                $groups[$group] = $meta->getReflectionProperty($group)->getValue($object);
            }
        }
        
        return $groups;
    }
    
    /**
     * Returns max position
     *
     * @param array $info Informations
     *
     * @return int
     */
    private function getMaxPosition(array $info)
    {
        if (!isset($this->maxPositions[$info['hash']])) {
            foreach ($info['groups'] as $group => $val) {
                if (is_object($val) && $this->objectManager->getUnitOfWork()->isScheduledForInsert($val)) {   
                    
                    return $this->maxPositions[$info['hash']] = -1;
                }
            }

            // Build request to retrieve max position
            $config = $this->configs[$info['meta']->name];
            $groups = isset($config['groups']) ? $config['groups'] : array();
            $qb = $this->objectManager->createQueryBuilder();
            $qb->select('MAX(n.'.$config['position'].')')
               ->from($config['useObjectClass'], 'n');

            $i = 1;
            foreach ($groups as $group) {
                $value = $info['meta']->getReflectionProperty($group)->getValue($info['object']);
                $whereFunc = is_null($qb->getDQLPart('where')) ? 'where' : 'andWhere';
                if (is_null($value)) {
                    $qb->{$whereFunc}($qb->expr()->isNull('n.'.$group));
                } else {
                    $qb->{$whereFunc}('n.'.$group.' = :group__'.$i);
                    $qb->setParameter('group__'.$i, $value);
                }
                $i++;
            }
            
            $query = $qb->getQuery();
            $query->useQueryCache(false);
            $query->useResultCache(false);
            $res = $query->getResult();
            
            $this->maxPositions[$info['hash']] = $res[0][1] === null ? -1 : $res[0][1]; 
        }

        return $this->maxPositions[$info['hash']];
    }
    
    /**
     * Compute new position
     *
     * @param array $info Informations to compute new position
     *
     * @return int
     */
    private function computeNewPosition(array $info)
    {
        $config = $this->configs[$info['meta']->name];
        
        // If new position is not defined : set -1
        $newPosition = $info['meta']->getReflectionProperty($config['position'])->getValue($info['object']);
        if (is_null($newPosition)) {
            $newPosition = -1;
        }
        
        // Get max position
        $maxPosition = $this->getMaxPosition($info);

        // Compute position if it is negative
        if ($newPosition < 0) {
            $min = -($maxPosition+2);
            $newPosition = max($newPosition, $min) - $min;
        }
     
        // Set position to max position if it is too big
        return min($newPosition, $maxPosition+1);
    }
    
    /**
     * Computes node positions and updates the sort field in memory and in the db
     *
     * @param array $info Informations to process deletion
     */
    private function processDeletion(array &$info)
    {
        // Retrieve data
        $config = $this->configs[$info['meta']->name];
        $position = $info['meta']->getReflectionProperty($config['position'])->getValue($info['object']);
        
        // Relocate
        $this->relocate($info, $position, -1);

        // Decrease max position
        if (isset($this->maxPositions[$info['hash']]) && $this->maxPositions[$info['hash']] > -1) {
            $this->maxPositions[$info['hash']]--;
        }
        
        // Synchronize objects in memory
        $uow = $this->objectManager->getUnitOfWork();
        foreach ($this->toProcess as &$otherInfo) {
            // Need to synchronize object in memory ?
            if ($info['meta']->name == $otherInfo['meta']->name && $info['hash'] == $otherInfo['hash'] && $info['identifier'] != $otherInfo['identifier']) {
                $objectInMemoryPosition = $otherInfo['meta']->getReflectionProperty($config['position'])->getValue($otherInfo['object']);
                if ($position <= $objectInMemoryPosition) {
                    $otherInfo['meta']->getReflectionProperty($config['position'])->setValue($otherInfo['object'], $objectInMemoryPosition-1);
                }
            }
        }
    }
    
    /**
     * Computes node positions and updates the sort field in memory and in the db
     *
     * @param array $info Informations to process insertion
     */
    private function processInsertion(array &$info)
    {
        // Compute new position
        $newPosition = $this->computeNewPosition($info);
        $config = $this->configs[$info['meta']->name];

        // Relocate
        $this->relocate($info, $newPosition, 1);
      
        // Increase max position
        $this->maxPositions[$info['hash']]++;
       
        // Synchronize objects in memory
        $notProcessed = false;
        foreach ($this->toProcess as &$otherInfo) {            
            if ($info['meta']->name == $otherInfo['meta']->name && $info['hash'] == $otherInfo['hash']) {    
                // Only deleted objects or not processed inserted objects are concerned
                if ($info['object'] === $otherInfo['object']) {
                    $notProcessed = true;
                }
                // Only if new position <= position of object in memory
                $objectInMemoryPosition = $otherInfo['meta']->getReflectionProperty($config['position'])->getValue($otherInfo['object']);
                if ($newPosition <= $objectInMemoryPosition) {
                    if ((!$notProcessed && $otherInfo['operation'] == self::TO_INSERT)
                        || $otherInfo['operation'] == self::TO_DELETE) {
                        $otherInfo['meta']->getReflectionProperty($config['position'])->setValue($otherInfo['object'], $objectInMemoryPosition+1);
                    }
                }
            }
        }

        // Set new position
        $info['meta']->getReflectionProperty($config['position'])->setValue($info['object'], $newPosition);
        $this->objectManager->getUnitOfWork()->recomputeSingleEntityChangeSet($info['meta'], $info['object']);
    }
    
    /**
     * Relocate
     *
     * @param array $info Info
     * @param int $start Inclusive index to start relocation from
     * @param int $delta The delta (positive or negative)
     */
    private function relocate(array $info, $start, $delta)
    {
        $uow = $this->objectManager->getUnitOfWork();
        
        // If a group is a new object, no relocation
        foreach ($info['groups'] as $group => $value) {
            if (null !== $value && is_object($value) && $uow->isScheduledForInsert($value)) {
                
                return;
            }
        }

        // Build query
        $config = $this->configs[$info['meta']->name];
        $sign = $delta < 0 ? '-' : '+';
        
        $dql = "UPDATE {$info['meta']->name} n";
        $dql .= " SET n.{$config['position']} = n.{$config['position']} {$sign} 1";
        $dql .= " WHERE n.{$config['position']} >= {$start}";
        
        // Add groups conditions
        $i = -1;
        $params = array();
        foreach ($info['groups'] as $group => $value) {
            if (null === $value) {
                $dql .= " AND n.{$group} IS NULL";
            } else {
                $dql .= " AND n.{$group} = :val___".(++$i);
                $params['val___'.$i] = $value;
            }
        }
        
        // If object is not new, add condition to ignore it 
        if (!$uow->isScheduledForInsert($info['object'])) {
            $identifiers = $info['meta']->getIdentifierFieldNames();
            $values = array();
            foreach ($identifiers as $id) {
                $idValue = $info['meta']->getReflectionProperty($id)->getValue($info['object']);
                if (null !== $idValue) {
                    $dql .= " AND n.{$id} != :id___".(++$i);
                    $params['id___'.$i] = $idValue;
                }
            }
        }
        
        $q = $this->objectManager->createQuery($dql);
        $q->setParameters($params);
        $q->getSingleScalarResult();
    }

    /**
     * {@inheritDoc}
     */
    protected function getNamespace()
    {
        return __NAMESPACE__;
    }
}