<?php declare(strict_types=1);
namespace Nevay\OTelInstrumentation\DoctrineDbal;

use Doctrine\DBAL\Driver\Result;
use Doctrine\DBAL\Driver\Statement;
use Doctrine\DBAL\ParameterType;
use OpenTelemetry\API\Trace\SpanBuilderInterface;
use function gettype;
use function is_int;

final class TracingStatement implements Statement {

    public function __construct(
        private readonly Statement $statement,
        private readonly SpanBuilderInterface $spanBuilder,
        private readonly DoctrineConfiguration $config,
    ) {}

    public function bindValue(int|string $param, mixed $value, ParameterType $type): void {
        $this->statement->bindValue($param, $value, $type);

        if ($this->config->captureParameters) {
            $this->spanBuilder->setAttribute(
                sprintf('db.operation.parameter.%s', is_int($param) ? $param - 1 : $param),
                match (gettype($value)) {
                    'NULL' => 'null',
                    'boolean' => $value ? 'true' : 'false',
                    default => (string) $value,
                },
            );
        }
    }

    public function execute(): Result {
        return Util::trace(
            $this->spanBuilder->startSpan(),
            $this->statement->execute(...),
        );
    }
}
