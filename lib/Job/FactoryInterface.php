<?php

namespace Resque\Job;

interface FactoryInterface
{
    /**
     * @param class-string<JobInterface> $className
     * @param array<string, mixed>|null $args
     * @param string $queue
     * @return \Resque\Job\JobInterface
     */
    public function create($className, $args, $queue);
}
