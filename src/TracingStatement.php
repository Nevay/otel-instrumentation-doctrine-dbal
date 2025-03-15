<?php declare(strict_types=1);
namespace Nevay\OTelInstrumentation\DoctrineDbal;

use Doctrine\DBAL\Driver\Result;
use Doctrine\DBAL\Driver\Statement;
use Doctrine\DBAL\ParameterType;
use OpenTelemetry\API\Trace\SpanBuilderInterface;
use function bin2hex;

final class TracingStatement implements Statement {

    public function __construct(
        private readonly Statement $statement,
        private readonly SpanBuilderInterface $spanBuilder,
        private readonly DoctrineConfiguration $config,
    ) {}

    public function bindValue(int|string $param, mixed $value, ParameterType $type): void {
        $this->statement->bindValue($param, $value, $type);

        if ($this->config->captureParameters) {
            $this->spanBuilder->setAttribute(sprintf('db.operation.parameter.%s', $param), match ($type) {
                ParameterType::INTEGER,
                ParameterType::ASCII,
                ParameterType::STRING => (string) $value,
                ParameterType::NULL => 'null',
                ParameterType::BOOLEAN => $value ? 'true' : 'false',
                ParameterType::BINARY => '0x' . bin2hex((string) $value),
                ParameterType::LARGE_OBJECT => null,
            });
        }
    }

    public function execute(): Result {
        return Util::trace(
            $this->spanBuilder->startSpan(),
            $this->statement->execute(...),
        );
    }
}
