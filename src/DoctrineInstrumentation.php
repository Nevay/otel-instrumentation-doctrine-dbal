<?php declare(strict_types=1);
namespace Nevay\OTelInstrumentation\DoctrineDbal;

use Doctrine\DBAL\Configuration;
use Doctrine\DBAL\Driver\Middleware;
use Doctrine\DBAL\DriverManager;
use OpenTelemetry\API\Configuration\ConfigProperties;
use OpenTelemetry\API\Instrumentation\AutoInstrumentation\Context;
use OpenTelemetry\API\Instrumentation\AutoInstrumentation\HookManagerInterface;
use OpenTelemetry\API\Instrumentation\AutoInstrumentation\Instrumentation;
use function array_filter;
use function array_unshift;

final class DoctrineInstrumentation implements Instrumentation {

    public function register(HookManagerInterface $hookManager, ConfigProperties $configuration, Context $context): void {
        $middleware = new TracingMiddleware($context->tracerProvider);

        $hookManager->hook(
            DriverManager::class,
            'getConnection',
            static function(string $class, array $params) use ($middleware): array {
                $config = $params[1] ?? new Configuration();

                $middlewares = $config->getMiddlewares();
                if (!array_filter($middlewares, static fn(Middleware $middleware) => $middleware instanceof TracingMiddleware)) {
                    array_unshift($middlewares, $middleware);
                    $config->setMiddlewares($middlewares);
                }

                return [1 => $config];
            }
        );
    }
}