<?php

namespace Resque\Job;

use Resque\Exceptions\ResqueException;

class Factory implements FactoryInterface
{
    /**
     * @param class-string<JobInterface> $className
     * @param array<string, mixed>|null $args
     * @param string $queue
     * @return \Resque\Job\JobInterface
     * @throws \Resque\Exceptions\ResqueException
     */
    public function create($className, $args, $queue)
    {
        if (!class_exists($className)) {
            throw new ResqueException(
                'Could not find job class ' . $className . '.'
            );
        }

        $instance = new $className();
        $instance->setArgs($args);
        $instance->setQueue($queue);
        return $instance;
    }
}
