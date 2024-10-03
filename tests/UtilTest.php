<?php declare(strict_types=1);
namespace Nevay\OTelInstrumentation\DoctrineDbal;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

#[CoversClass(Util::class)]
final class UtilTest extends TestCase {

    #[DataProvider('sanitizeProvider')]
    public function testSanitize(string $sql, string $sanitized): void {
        $this->assertSame($sanitized, Util::attributes($sql)['db.query.text']);
    }

    public static function sanitizeProvider(): iterable {
        yield [
            "select *, 'abc' from user join order on user.id = order.user_id and user.active = 1 where order.ordernumber = '00001'",
            'select *, ? from user join order on user.id = order.user_id and user.active = ? where order.ordernumber = ?',
        ];
        yield [
            "update user set first_name = 'Jane', last_name = 'Doe' where id = 1",
            'update user set first_name = ?, last_name = ? where id = ?',
        ];
    }
}
