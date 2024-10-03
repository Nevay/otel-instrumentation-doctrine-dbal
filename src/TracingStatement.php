<?php declare(strict_types=1);
namespace Nevay\OTelInstrumentation\DoctrineDbal;

use Doctrine\DBAL\Driver\Result;
use Doctrine\DBAL\Driver\Statement;
use Doctrine\DBAL\ParameterType;
use OpenTelemetry\API\Trace\SpanBuilderInterface;

final class TracingStatement implements Statement {

    public function __construct(
        private readonly Statement $statement,
        private readonly SpanBuilderInterface $spanBuilder,
    ) {}

    public function bindValue(int|string $param, mixed $value, ParameterType $type): void {
        $this->statement->bindValue($param, $value, $type);
    }

    public function execute(): Result {
        $span = (clone $this->spanBuilder)
            ->setAttribute('code.function', __FUNCTION__)
            ->setAttribute('code.namespace', $this->statement::class)
            ->startSpan();

        return Util::trace($span, $this->statement->execute(...));
    }
}
