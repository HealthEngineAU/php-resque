<?php

namespace Resque;

use Psr\Log\AbstractLogger;
use Psr\Log\LogLevel;
use Stringable;

/**
 * Resque default logger PSR-3 compliant
 *
 * @package		Resque/Stat
 * @author		Chris Boulton <chris@bigcommerce.com>
 * @license		http://www.opensource.org/licenses/mit-license.php
 */
class Logger extends AbstractLogger
{
    public function __construct(public bool $verbose = false) {}

    /**
     * @param mixed $level
     * @param string|Stringable $message
     * @param array<string, string> $context
     * @return void
     */
    public function log($level, string|Stringable $message, array $context = []): void
    {
        if ($this->verbose) {
            fwrite(
                STDOUT,
                '[' . $level . '] [' . date('H:i:s Y-m-d') . '] ' . $this->interpolate($message, $context) . PHP_EOL
            );
            return;
        }

        if (!($level === LogLevel::INFO || $level === LogLevel::DEBUG)) {
            fwrite(
                STDOUT,
                '[' . $level . '] ' . $this->interpolate($message, $context) . PHP_EOL
            );
        }
    }

    /**
     * Fill placeholders with the provided context
     * @author Jordi Boggiano j.boggiano@seld.be
     *
     * @param  string  $message  Message to be logged
     * @param  array<string, string>   $context  Array of variables to use in message
     * @return string
     */
    public function interpolate($message, array $context = [])
    {
        // build a replacement array with braces around the context keys
        $replace = array();
        foreach ($context as $key => $val) {
            $replace['{' . $key . '}'] = $val;
        }

        // interpolate replacement values into the message and return
        return strtr($message, $replace);
    }
}
