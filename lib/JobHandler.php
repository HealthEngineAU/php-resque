<?php

namespace Resque;

use Resque\Job\JobInterface;
use Resque\Job\PID;
use Resque\Job\Status;
use Resque\Exceptions\DoNotPerformException;
use Resque\Job\FactoryInterface;
use Resque\Job\Factory;
use Error;
use Resque\Worker\ResqueWorker;

/**
 * Resque job.
 *
 * @package		Resque/JobHandler
 * @author		Chris Boulton <chris@bigcommerce.com>
 * @license		http://www.opensource.org/licenses/mit-license.php
 */
class JobHandler
{
    /**
     * @var string The name of the queue that this job belongs to.
     */
    public $queue;

    /**
     * @var ResqueWorker|null Instance of the Resque worker running this job.
     */
    public $worker;

    /**
     * @var array{
     *     args?: array{
     *         0?: array<string, mixed>|null
     *     },
     *     class: class-string<JobInterface>,
     *     id?: string,
     *     prefix?: string
     * } Array containing details of the job.
     */
    public $payload;

    /**
     * @var \Resque\Job\JobInterface|null Instance of the class performing work for this job.
     */
    private $instance;

    /**
     * @var \Resque\Job\FactoryInterface|null
     */
    private $jobFactory;

    /**
     * Instantiate a new instance of a job.
     *
     * @param string $queue The queue that the job belongs to.
     * @param array{
     *     args?: array{
     *         0?: array<string, mixed>|null
     *     },
     *     class: class-string<JobInterface>,
     *     id?: string,
     *     prefix?: string
     * } $payload array containing details of the job.
     */
    public function __construct($queue, $payload)
    {
        $this->queue = $queue;
        $this->payload = $payload;
    }

    /**
     * Create a new job and save it to the specified queue.
     *
     * @param string $queue The name of the queue to place the job in.
     * @param class-string<JobInterface> $class The name of the class that contains the code to execute the job.
     * @param array<string, mixed>|null $args Any optional arguments that should be passed when the job is executed.
     * @param boolean $monitor Set to true to be able to monitor the status of a job.
     * @param string|null $id Unique identifier for tracking the job. Generated if not supplied.
     * @param string $prefix The prefix needs to be set for the status key
     *
     * @return string
     * @throws \InvalidArgumentException
     */
    public static function create($queue, $class, ?array $args = null, $monitor = false, $id = null, $prefix = "")
    {
        if (is_null($id)) {
            $id = Resque::generateJobId();
        }

        Resque::push($queue, array(
            'class'	     => $class,
            'args'	     => array($args),
            'id'	     => $id,
            'prefix'     => $prefix,
            'queue_time' => microtime(true),
        ));

        if ($monitor) {
            Status::create($id, $prefix);
        }

        return $id;
    }

    /**
     * Find the next available job from the specified queue and return an
     * instance of JobHandler for it.
     *
     * @param string $queue The name of the queue to check for a job in.
     * @return false|JobHandler Null when there aren't any waiting jobs, instance of Resque\JobHandler when a job was found.
     */
    public static function reserve($queue)
    {
        $payload = Resque::pop($queue);

        if (!is_array($payload)) {
            return false;
        }

        return new JobHandler($queue, $payload);
    }

    /**
     * Find the next available job from the specified queues using blocking list pop
     * and return an instance of JobHandler for it.
     *
     * @param   string[]          $queues
     * @param   int               $timeout
     * @return  false|JobHandler  False when there aren't any waiting jobs, instance of Resque\JobHandler when a job was found.
     */
    public static function reserveBlocking(array $queues, $timeout = null)
    {
        $item = Resque::blpop($queues, $timeout);

        if (!is_array($item)) {
            return false;
        }

        /**
         * @var array{
         *     payload: array{
         *         args?: array{
         *             0?: array<string, mixed>|null
         *         },
         *         class: class-string<JobInterface>,
         *         id?: string,
         *         prefix?: string
         *     },
         *     queue: string
         * } $item
         */

        return new JobHandler($item['queue'], $item['payload']);
    }

    /**
     * Update the status of the current job.
     *
     * @param int $status Status constant from Resque\Job\Status indicating the current status of a job.
     * @param bool|null $result
     */
    public function updateStatus(int $status, $result = null): void
    {
        if (!array_key_exists('id', $this->payload)) {
            return;
        }

        $statusInstance = new Status($this->payload['id'], $this->getPrefix());
        $statusInstance->update($status, $result);
    }

    /**
     * Return the status of the current job.
     *
     * @return false|int|null The status of the job as one of the Resque\Job\Status constants
     *                  or null if job is not being tracked.
     */
    public function getStatus()
    {
        if (!array_key_exists('id', $this->payload)) {
            return null;
        }

        $status = new Status($this->payload['id'], $this->getPrefix());
        return $status->get();
    }

    /**
     * Get the arguments supplied to this job.
     *
     * @return array<string, mixed>|null Array of arguments.
     */
    public function getArguments()
    {
        if (
            !array_key_exists('args', $this->payload)
            || !array_key_exists(0, $this->payload['args'])
            || $this->payload['args'][0] === null
        ) {
            return null;
        }

        return $this->payload['args'][0];
    }

    /**
     * Get the instantiated object for this job that will be performing work.
     * @return \Resque\Job\JobInterface Instance of the object that this job belongs to.
     * @throws \Resque\Exceptions\ResqueException
     */
    public function getInstance()
    {
        $job = $this->instance;

        if ($job !== null) {
            return $job;
        }

        $job = $this->getJobFactory()->create($this->payload['class'], $this->getArguments(), $this->queue);
        $job->setJobHandler($this);

        $this->instance = $job;

        return $job;
    }

    /**
     * Actually execute a job by calling the perform method on the class
     * associated with the job with the supplied arguments.
     *
     * @return bool
     * @throws \Resque\Exceptions\ResqueException When the job's class could not be found
     * 											 or it does not contain a perform method.
     */
    public function perform()
    {
        $result = true;
        try {
            Event::trigger('beforePerform', $this);

            $instance = $this->getInstance();
            if (is_callable([$instance, 'setUp'])) {
                $instance->setUp();
            }

            $result = $instance->perform();

            if (is_callable([$instance, 'tearDown'])) {
                $instance->tearDown();
            }

            Event::trigger('afterPerform', $this);
        } catch (DoNotPerformException $e) {
            // beforePerform/setUp have said don't perform this job. Return.
            $result = false;
        }

        return $result;
    }

    /**
     * Mark the current job as having failed.
     *
     * @param \Error|\Exception $exception
     */
    public function fail($exception): void
    {
        $worker = $this->worker;

        if ($worker === null) {
            throw new \RuntimeException('Worker is null so cannot fail the job');
        }

        Event::trigger('onFailure', array(
            'exception' => $exception,
            'job' => $this,
        ));

        $this->updateStatus(Status::STATUS_FAILED);
        if ($exception instanceof Error) {
            FailureHandler::createFromError(
                $this->payload,
                $exception,
                $worker,
                $this->queue
            );
        } else {
            FailureHandler::create(
                $this->payload,
                $exception,
                $worker,
                $this->queue
            );
        }

        if (array_key_exists('id', $this->payload)) {
            PID::del($this->payload['id']);
        }

        Stat::incr('failed');
        Stat::incr('failed:' . $this->worker);
    }

    /**
     * Re-queue the current job.
     * @return string
     */
    public function recreate()
    {
        $monitor = false;
        if (array_key_exists('id', $this->payload)) {
            $status = new Status($this->payload['id'], $this->getPrefix());
            if ($status->isTracking()) {
                $monitor = true;
            }
        }

        return self::create(
            $this->queue,
            $this->payload['class'],
            $this->getArguments(),
            $monitor,
            null,
            $this->getPrefix()
        );
    }

    /**
     * Generate a string representation used to describe the current job.
     *
     * @return string The string representation of the job.
     */
    public function __toString()
    {
        $name = array(
            'Job{' . $this->queue . '}'
        );
        if (array_key_exists('id', $this->payload)) {
            $name[] = 'ID: ' . $this->payload['id'];
        }
        $name[] = $this->payload['class'];
        if (array_key_exists('args', $this->payload)) {
            $name[] = json_encode($this->payload['args']);
        }
        return '(' . implode(' | ', $name) . ')';
    }

    /**
     * @param \Resque\Job\FactoryInterface $jobFactory
     * @return \Resque\JobHandler
     */
    public function setJobFactory(FactoryInterface $jobFactory)
    {
        $this->jobFactory = $jobFactory;

        return $this;
    }

    /**
     * @return \Resque\Job\FactoryInterface
     */
    public function getJobFactory()
    {
        $jobFactory = $this->jobFactory;

        if ($jobFactory !== null) {
            return $jobFactory;
        }

        $jobFactory = new Factory();
        $this->jobFactory = $jobFactory;

        return $jobFactory;
    }

    /**
     * @return string
     */
    private function getPrefix()
    {
        if (isset($this->payload['prefix'])) {
            return $this->payload['prefix'];
        }

        return '';
    }
}
