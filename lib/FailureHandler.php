<?php

namespace Resque;

use Resque\Failure\RedisFailure;
use Resque\Worker\ResqueWorker;
use Exception;
use Error;

/**
 * Failed Resque job.
 *
 * @package		Resque/FailureHandler
 * @author		Chris Boulton <chris@bigcommerce.com>
 * @license		http://www.opensource.org/licenses/mit-license.php
 */
class FailureHandler
{
    /**
     * @var class-string|null Class name representing the backend to pass failed jobs off to.
     */
    private static ?string $backend = null;

    /**
     * Create a new failed job on the backend.
     *
     * @param array<string, mixed> $payload        The contents of the job that has just failed.
     * @param Exception $exception  The exception generated when the job failed to run.
     * @param ResqueWorker $worker Instance of Resque\Worker\ResqueWorker
     *											  that was running this job when it failed.
     * @param string $queue          The name of the queue that this job was fetched from.
     */
    public static function create(array $payload, Exception $exception, ResqueWorker $worker, string $queue): void
    {
        $backend = self::getBackend();
        new $backend($payload, $exception, $worker, $queue);
    }

    /**
     * Create a new failed job on the backend from PHP errors.
     *
     * @param  array<string, mixed>  $payload    The contents of the job that has just failed.
     * @param  Error                 $exception  The PHP error generated when the job failed to run.
     * @param  ResqueWorker          $worker     The Worker instance that was running this job when it failed.
     * @param  string                $queue      The name of the queue that this job was fetched from.
     */
    public static function createFromError(array $payload, Error $exception, ResqueWorker $worker, string $queue): void
    {
        $backend = self::getBackend();
        new $backend($payload, $exception, $worker, $queue);
    }

    /**
     * Return an instance of the backend for saving job failures.
     *
     * @return class-string Class name of back-end.
     */
    public static function getBackend(): string
    {
        if (self::$backend === null) {
            self::$backend = RedisFailure::class;
        }

        return self::$backend;
    }

    /**
     * Set the backend class to use for raised job failures. The supplied backend
     * should be the name of a class to be instantiated when a job fails.
     * It is your responsibility to have the backend class loaded (or autoloaded)
     *
     * @param class-string $backend The class name of the backend to pipe failures to.
     */
    public static function setBackend(string $backend): void
    {
        self::$backend = $backend;
    }
}
