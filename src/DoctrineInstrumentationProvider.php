<?php declare(strict_types=1);
namespace Nevay\OTelInstrumentation\DoctrineDbal;

use Nevay\OTelSDK\Configuration\ComponentProvider;
use Nevay\OTelSDK\Configuration\ComponentProviderRegistry;
use Nevay\OTelSDK\Configuration\Context;
use OpenTelemetry\API\Instrumentation\AutoInstrumentation\InstrumentationConfiguration;
use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;
use Symfony\Component\Config\Definition\Builder\NodeBuilder;

/**
 * @implements ComponentProvider<InstrumentationConfiguration>
 */
final class DoctrineInstrumentationProvider implements ComponentProvider {

    /**
     * @param array{
     *     captureParameters: bool,
     * } $properties
     */
    public function createPlugin(array $properties, Context $context): InstrumentationConfiguration {
        return new DoctrineConfiguration(
            captureParameters: $properties['captureParameters'],
        );
    }

    public function getConfig(ComponentProviderRegistry $registry, NodeBuilder $builder): ArrayNodeDefinition {
        $node = $builder->arrayNode('doctrine');
        $node
            ->children()
                ->booleanNode('captureParameters')->defaultFalse()->end()
            ->end()
        ;

        return $node;
    }
}
