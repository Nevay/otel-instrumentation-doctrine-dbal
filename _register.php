<?php declare(strict_types=1);
namespace Nevay\OTelInstrumentation\DoctrineDbal;

use Nevay\SPI\ServiceLoader;
use OpenTelemetry\API\Configuration\Config\ComponentProvider;
use OpenTelemetry\API\Configuration\ConfigEnv\EnvComponentLoader;
use OpenTelemetry\API\Instrumentation\AutoInstrumentation\Instrumentation;
use function class_exists;

if (!class_exists(ServiceLoader::class)) {
    return;
}

ServiceLoader::register(Instrumentation::class, DoctrineInstrumentation::class);

ServiceLoader::register(ComponentProvider::class, Config\InstrumentationConfigurationDoctrineDbalConfig::class);

ServiceLoader::register(EnvComponentLoader::class, ConfigEnv\InstrumentationConfigurationDoctrineDbalConfig::class);
