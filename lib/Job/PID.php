<?php

namespace Resque\Job;

use Resque\Resque;

/**
 * PID tracker for the forked worker job.
 *
 * @package		Resque/Job
 * @author		Chris Boulton <chris@bigcommerce.com>
 * @license		http://www.opensource.org/licenses/mit-license.php
 */
class PID
{
    /**
     * Create a new PID tracker item for the supplied job ID.
     *
     * @param string $id The ID of the job to track the PID of.
     */
    public static function create($id): void
    {
        Resque::redis()->set('job:' . $id . ':pid', (string)getmypid());
    }

    /**
     * Fetch the PID for the process actually executing the job.
     *
     * @param string $id The ID of the job to get the PID of.
     *
     * @return int PID of the process doing the job (on non-forking OS, PID of the worker, otherwise forked PID).
     */
    public static function get($id)
    {
        $pid = Resque::redis()->get('job:' . $id . ':pid');

		if ($pid === false) {
			return 0;
		}

        if (!is_string($pid)) {
            throw new \UnexpectedValueException('Did not expect PID to be of type: ' . gettype($pid));
        }

        return (int)$pid;
    }

    /**
     * Remove the PID tracker for the job.
     *
     * @param string $id The ID of the job to remove the tracker from.
     */
    public static function del($id): void
    {
        Resque::redis()->del('job:' . $id . ':pid');
    }
}
