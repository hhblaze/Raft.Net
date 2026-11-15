import { test, expect } from '@playwright/test';
import { createHmac } from 'node:crypto';
import fs from 'node:fs';

const settings = JSON.parse(fs.readFileSync(process.env.RAFT_DASHBOARD_TEST_SETTINGS!, 'utf8').replace(/^\uFEFF/, '')) as { Url: string; Password: string; Secure?: boolean };
test('secure stand requires TOTP before administration and then shows real TLS nodes', async ({ browser }, testInfo) => {
  test.skip(!settings.Secure, 'Use the owned HTTPS stand settings.');
  // The lab CA is deliberately not installed in system/browser trust. Only this test context accepts it.
  const context = await browser.newContext({ ignoreHTTPSErrors: true });
  const page = await context.newPage();
  await page.goto(settings.Url);
  await page.getByLabel('Имя пользователя').fill('admin');
  await page.getByLabel('Пароль', { exact: true }).fill(settings.Password);
  await page.getByRole('button', { name: 'Войти', exact: true }).click();
  await expect(page.getByText('Завершите настройку аутентификатора для управления кластерами.')).toBeVisible();
  expect((await context.request.get(settings.Url + '/api/admin/users')).status()).toBe(403);
  await page.getByRole('button', { name: 'Настроить аутентификатор', exact: true }).click();
  await page.getByRole('dialog').getByLabel('Пароль', { exact: true }).fill(settings.Password);
  await page.getByRole('button', { name: 'Создать ключ аутентификатора', exact: true }).click();
  const secret = await page.getByRole('dialog').locator('.mono').innerText();
  const alphabet = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ234567'; const bytes: number[] = []; let bits = 0, value = 0;
  for (const character of secret.trim().replace(/=+$/, '')) { value = (value << 5) | alphabet.indexOf(character); bits += 5; if (bits >= 8) { bits -= 8; bytes.push((value >>> bits) & 255); } }
  const counter = Buffer.alloc(8); counter.writeBigUInt64BE(BigInt(Math.floor(Date.now() / 30000)));
  const hash = createHmac('sha1', Buffer.from(bytes)).update(counter).digest(); const offset = hash[19] & 15;
  const code = String((hash.readUInt32BE(offset) & 0x7fffffff) % 1000000).padStart(6, '0');
  await page.getByRole('dialog').getByLabel('Код аутентификатора', { exact: true }).fill(code);
  await page.getByRole('button', { name: 'Проверить и включить', exact: true }).click();
  await expect(page.getByText('Сохраните коды восстановления. Каждый действует один раз.')).toBeVisible();
  await page.keyboard.press('Escape');
  // The modal animates out. Never capture recovery codes or the fading backdrop in evidence.
  await expect(page.locator('[role="dialog"]:visible')).toHaveCount(0);
  await expect(page.locator('.ant-modal-mask:visible')).toHaveCount(0);
  await expect(page.locator('.node-card')).toHaveCount(3, { timeout: 30_000 });
  await expect(page.locator('.stale-alert')).toHaveCount(0, { timeout: 30_000 });
  await page.screenshot({ path: testInfo.outputPath('secure-totp-overview.png'), fullPage: true });
  await context.close();
});
