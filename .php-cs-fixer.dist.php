<?php

declare(strict_types=1);

use PhpCsFixer\Config;
use PhpCsFixer\Finder;

return (new Config())
    ->setFinder(Finder::create()->in(__DIR__)->name(['resque']))
    ->setRules([
        '@PER' => true,
    ]);
