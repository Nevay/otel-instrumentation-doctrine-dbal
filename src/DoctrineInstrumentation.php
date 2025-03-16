<?php declare(strict_types=1);
namespace Nevay\OTelInstrumentation\DoctrineDbal;

use Doctrine\DBAL\Configuration;
use Doctrine\DBAL\DriverManager;
use OpenTelemetry\API\Configuration\ConfigProperties;
use OpenTelemetry\API\Instrumentation\AutoInstrumentation\Context;
use OpenTelemetry\API\Instrumentation\AutoInstrumentation\HookManagerInterface;
use OpenTelemetry\API\Instrumentation\AutoInstrumentation\Instrumentation;
use function array_unshift;

final class DoctrineInstrumentation implements Instrumentation {

    public function register(HookManagerInterface $hookManager, ConfigProperties $configuration, Context $context): void {
        $config = $configuration->get(DoctrineConfiguration::class) ?? new DoctrineConfiguration();
        if (!$config->enabled) {
            return;
        }

        $middleware = new TracingMiddleware($context->tracerProvider, $config);

        $hookManager->hook(
            DriverManager::class,
            'getConnection',
            static function(string $class, array $params) use ($middleware): array {
                $config = $params[1] ?? new Configuration();
                if (!$config instanceof Configuration) {
                    return [];
                }

                $middlewares = $config->getMiddlewares();
                foreach ($middlewares as $middleware) {
                    if ($middleware instanceof TracingMiddleware) {
                        return [];
                    }
                }

                array_unshift($middlewares, $middleware);
                $config->setMiddlewares($middlewares);

                return [1 => $config];
            }
        );
    }
}
