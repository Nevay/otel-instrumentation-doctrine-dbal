<?php declare(strict_types=1);
namespace Nevay\OTelInstrumentation\DoctrineDbal\Config;

use Nevay\OTelInstrumentation\DoctrineDbal\DoctrineConfiguration;
use OpenTelemetry\API\Configuration\Config\ComponentProvider;
use OpenTelemetry\API\Configuration\Config\ComponentProviderRegistry;
use OpenTelemetry\API\Configuration\Context;
use OpenTelemetry\API\Instrumentation\AutoInstrumentation\InstrumentationConfiguration;
use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;
use Symfony\Component\Config\Definition\Builder\NodeBuilder;

/**
 * @implements ComponentProvider<InstrumentationConfiguration>
 */
final class InstrumentationConfigurationDoctrineDbalConfig implements ComponentProvider {

    /**
     * @param array{
     *     enabled: bool,
     *     capture_parameters: bool,
     * } $properties
     */
    public function createPlugin(array $properties, Context $context): InstrumentationConfiguration {
        return new DoctrineConfiguration(
            enabled: $properties['enabled'],
            captureParameters: $properties['capture_parameters'],
        );
    }

    public function getConfig(ComponentProviderRegistry $registry, NodeBuilder $builder): ArrayNodeDefinition {
        $node = $builder->arrayNode('doctrine_dbal');
        $node
            ->canBeDisabled()
            ->children()
                ->booleanNode('capture_parameters')->defaultFalse()->end()
            ->end()
        ;

        return $node;
    }
}
