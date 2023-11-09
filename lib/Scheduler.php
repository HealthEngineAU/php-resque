<?php

namespace Resque;

use DateTimeInterface;
use Resque\Exceptions\ResqueException;
use Resque\Exceptions\InvalidTimestampException;
use DateTime;

/**
* Resque scheduler core class to handle scheduling of jobs in the future.
*
* @package		Resque/Scheduler
* @author		Chris Boulton <chris@bigcommerce.com>
* @copyright	(c) 2012 Chris Boulton
* @license		http://www.opensource.org/licenses/mit-license.php
*/
class Scheduler
{
    public const VERSION = "0.1";

    /**
     * Enqueue a job in a given number of seconds from now.
     *
     * Identical to Resque::enqueue, however the first argument is the number
     * of seconds before the job should be executed.
     *
     * @param int $in Number of seconds from now when the job should be executed.
     * @param string $queue The name of the queue to place the job in.
     * @param class-string $class The name of the class that contains the code to execute the job.
     * @param array<string, mixed> $args Any optional arguments that should be passed when the job is executed.
     */
    public static function enqueueIn($in, $queue, $class, array $args = array()): void
    {
        self::enqueueAt(time() + $in, $queue, $class, $args);
    }

    /**
     * Enqueue a job for execution at a given timestamp.
     *
     * Identical to Resque::enqueue, however the first argument is a timestamp
     * (either UNIX timestamp in integer format or an instance of the DateTime
     * class in PHP).
     *
     * @param \DateTimeInterface|int $at Instance of PHP DateTime object or int of UNIX timestamp.
     * @param string $queue The name of the queue to place the job in.
     * @param class-string $class The name of the class that contains the code to execute the job.
     * @param array<string, mixed> $args Any optional arguments that should be passed when the job is executed.
     * @throws ResqueException
     */
    public static function enqueueAt($at, $queue, $class, $args = array()): void
    {
        self::validateJob($class, $queue);

        $job = self::jobToHash($queue, $class, $args);
        self::delayedPush($at, $job);

        Event::trigger('afterSchedule', array(
            'at'    => $at,
            'queue' => $queue,
            'class' => $class,
            'args'  => $args,
        ));
    }

    /**
     * Directly append an item to the delayed queue schedule.
     *
     * @param \DateTimeInterface|int $timestamp Timestamp job is scheduled to be run at.
     * @param array{
     *     args: array{
     *         0: array<string, mixed>
     *     },
     *     class: class-string,
     *     queue: string
     * } $item Hash of item to be pushed to schedule.
     */
    public static function delayedPush($timestamp, $item): void
    {
        $timestamp = self::getTimestamp($timestamp);
        $redis = Resque::redis();
        $redis->rpush('delayed:' . $timestamp, json_encode($item));
        $redis->zadd('delayed_queue_schedule', $timestamp, $timestamp);
    }

    /**
     * Get the total number of jobs in the delayed schedule.
     *
     * @return int Number of scheduled jobs.
     */
    public static function getDelayedQueueScheduleSize()
    {
        $setCardinality = Resque::redis()->zcard('delayed_queue_schedule');

        if ($setCardinality === false) {
            return 0;
        }

        if (!is_int($setCardinality)) {
            throw new \UnexpectedValueException(
                'Did not expect set cardinality to be of type: ' . gettype($setCardinality)
            );
        }

        return $setCardinality;
    }

    /**
     * Get the number of jobs for a given timestamp in the delayed schedule.
     *
     * @param \DateTime|int $timestamp Timestamp
     * @return int Number of scheduled jobs.
     */
    public static function getDelayedTimestampSize($timestamp)
    {
        $timestamp = self::getTimestamp($timestamp);

        $listLength = Resque::redis()->llen('delayed:' . $timestamp);

        if ($listLength === false) {
            return 0;
        }

        if (!is_int($listLength)) {
            throw new \UnexpectedValueException(
                'Did not expect list length to be of type: ' . gettype($listLength)
            );
        }

        return $listLength;
    }

    /**
     * Remove a delayed job from the queue
     *
     * note: you must specify exactly the same
     * queue, class and arguments that you used when you added
     * to the delayed queue
     *
     * also, this is an expensive operation because all delayed keys have to be
     * searched
     *
     * @param string $queue
     * @param class-string $class
     * @param array<string, mixed> $args
     * @return int number of jobs that were removed
     * @throws \JsonException
     */
    public static function removeDelayed($queue, $class, $args)
    {
        $destroyed = 0;
        $item = json_encode(self::jobToHash($queue, $class, $args), JSON_THROW_ON_ERROR);
        $redis = Resque::redis();
        $keys = $redis->keys('delayed:*');

        if (!is_array($keys)) {
            throw new \UnexpectedValueException('Did not expect keys to be of type: ' . gettype($keys));
        }

        foreach ($keys as $key) {
            $key = Redis::removePrefix($key);
            $destroyed += $redis->lrem($key, $item);
        }

        return $destroyed;
    }

    /**
     * removed a delayed job queued for a specific timestamp
     *
     * note: you must specify exactly the same
     * queue, class and arguments that you used when you added
     * to the delayed queue
     *
     * @param \DateTimeInterface|int $timestamp
     * @param string $queue
     * @param class-string $class
     * @param array<string, mixed> $args
     * @return mixed
     * @throws \JsonException
     */
    public static function removeDelayedJobFromTimestamp($timestamp, $queue, $class, $args)
    {
        $key = 'delayed:' . self::getTimestamp($timestamp);
        $item = json_encode(self::jobToHash($queue, $class, $args), JSON_THROW_ON_ERROR);
        $redis = Resque::redis();
        $count = $redis->lrem($key, $item);
        self::cleanupTimestamp($key, $timestamp);

        return $count;
    }

    /**
     * Generate hash of all job properties to be saved in the scheduled queue.
     *
     * @param string $queue Name of the queue the job will be placed on.
     * @param class-string $class Name of the job class.
     * @param array<string, mixed> $args Array of job arguments.
     * @return array{
     *      args: array{
     *         0: array<string, mixed>
     *      },
     *      class: class-string,
     *      queue: string
     *  }
     */
    private static function jobToHash($queue, $class, $args)
    {
        return [
            'args'  => [$args],
            'class' => $class,
            'queue' => $queue,
        ];
    }

    /**
     * If there are no jobs for a given key/timestamp, delete references to it.
     *
     * Used internally to remove empty delayed: items in Redis when there are
     * no more jobs left to run at that timestamp.
     *
     * @param string $key Key to count number of items at.
     * @param DateTimeInterface|int $timestamp Matching timestamp for $key.
     */
    private static function cleanupTimestamp($key, $timestamp): void
    {
        $timestamp = self::getTimestamp($timestamp);
        $redis = Resque::redis();

        if ($redis->llen($key) == 0) {
            $redis->del($key);
            $redis->zrem('delayed_queue_schedule', $timestamp);
        }
    }

    /**
     * Convert a timestamp in some format in to a unix timestamp as an integer.
     *
     * @param DateTimeInterface|int $timestamp Instance of DateTime or UNIX timestamp.
     * @return int Timestamp
     */
    private static function getTimestamp(DateTimeInterface|int $timestamp): int
    {
        if ($timestamp instanceof DateTimeInterface) {
            return $timestamp->getTimestamp();
        }

        return $timestamp;
    }

    /**
     * Find the first timestamp in the delayed schedule before/including the timestamp.
     *
     * Will find and return the first timestamp upto and including the given
     * timestamp. This is the heart of the Scheduler that will make sure
     * that any jobs scheduled for the past when the worker wasn't running are
     * also queued up.
     *
     * @param \DateTimeInterface|int|null $at Instance of DateTimeInterface or UNIX timestamp.
     *                                Defaults to now.
     * @return int|false UNIX timestamp, or false if nothing to run.
     */
    public static function nextDelayedTimestamp(DateTimeInterface|int|null $at = null): false|int
    {
        if ($at === null) {
            $at = time();
        } else {
            $at = self::getTimestamp($at);
        }

        $items = Resque::redis()->zrangebyscore('delayed_queue_schedule', '-inf', (string)$at, array('limit' => array(0, 1)));

        if (is_array($items)) {
            return $items[0];
        }

        return false;
    }

    /**
     * Pop a job off the delayed queue for a given timestamp.
     *
     * @param \DateTimeInterface|int $timestamp Instance of DateTime or UNIX timestamp.
     * @return array{
     *     args: array<string, mixed>,
     *     class: class-string,
     *     queue: string
     * }|false Matching job at timestamp.
     */
    public static function nextItemForTimestamp($timestamp)
    {
        $timestamp = self::getTimestamp($timestamp);
        $key = 'delayed:' . $timestamp;

        /** @var false|string $value */
        $value = Resque::redis()->lpop($key);

        if ($value === false) {
            return false;
        }

        /**
         * @var array{
         *     args: array<string, mixed>,
         *     class: class-string,
         *     queue: string
         * } $item
         */
        $item = json_decode($value, true);

        self::cleanupTimestamp($key, $timestamp);

        return $item;
    }

    /**
     * Ensure that supplied job class/queue is valid.
     *
     * @param class-string $class Name of job class.
     * @param string $queue Name of queue.
     * @throws \Resque\Exceptions\ResqueException
     */
    private static function validateJob(string $class, string $queue): bool
    {
        if (trim($class) === '') {
            throw new ResqueException('Jobs must be given a class.');
        } elseif (trim($queue) === '') {
            throw new ResqueException('Jobs must be put in a queue.');
        }

        return true;
    }
}
