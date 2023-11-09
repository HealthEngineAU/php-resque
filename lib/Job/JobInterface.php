<?php

namespace Resque\Job;

use Resque\JobHandler;

interface JobInterface
{
    public function perform(): bool;

    /**
     * @param array<string, mixed>|null $args
     * @return void
     */
    public function setArgs(array|null $args): void;

    public function setJobHandler(JobHandler $jobHandler): void;

    public function setQueue(string $queue): void;
}
