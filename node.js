require('dotenv').config();
const Redis = require('ioredis');
const redis = new Redis({
  host: process.env.REDIS_HOST,
  port: process.env.REDIS_PORT,
  password: process.env.REDIS_PASSWORD
});
const fs = require('fs');
const moment = require('moment');

const BATCH_SIZE = 1000;

async function processLogsEfficiently(logFilePath, eventType, keyPrefix, valueKey) {
  try {
    const data = fs.readFileSync(logFilePath, 'utf8');
    const logs = JSON.parse(data);

    let pipeline = redis.pipeline();
    let processedCount = 0;

    for (const log of logs) {
      if (log.event === eventType && log[valueKey]) {
        const date = moment(log.timestamp).format('YYYY-MM-DD');
        pipeline.pfadd(`${keyPrefix}:${date}`, log[valueKey]);
        processedCount++;
      }

      if (pipeline.length >= BATCH_SIZE) {
        await pipeline.exec();
        // pipeline.clear();
        pipeline = redis.pipeline(); 
      }
    }

    if (pipeline.length > 0) {
      await pipeline.exec();
    }

    console.log(`Đã xử lý ${processedCount} logs từ ${logFilePath} cho ${keyPrefix}`);

  } catch (error) {
    console.error(`Lỗi xử lý file ${logFilePath}:`, error);
  }
}

async function processLoginForRR1Efficiently(logFilePath) {
  try {
    const data = fs.readFileSync(logFilePath, 'utf8');
    const logs = JSON.parse(data);

    let pipeline = redis.pipeline();
    let processedCount = 0;

    for (const log of logs) {
      if (log.event === "login" && log.user_id) {
        const date = moment(log.timestamp).format('YYYY-MM-DD');
        pipeline.sadd(`login_users:${date}`, log.user_id);
        processedCount++;
      }

      if (pipeline.length >= BATCH_SIZE) {
        await pipeline.exec();
        // pipeline.clear();
        pipeline = redis.pipeline(); 
      }
    }

    if (pipeline.length > 0) {
      await pipeline.exec();
    }

    console.log(`Đã xử lý ${processedCount} logs từ ${logFilePath} cho RR1 tracking`);

  } catch (error) {
    console.error(`Lỗi xử lý file ${logFilePath} (RR1):`, error);
  }
}

// Lấy giá trị ước lượng từ HyperLogLog
async function getHLLCount(key) {
  return await redis.pfcount(key);
}

// Tính RR1 trên Redis
async function calculateRR1Efficiently() {
  const rr1 = {};
  const keys = await redis.keys('login_users:*');
  const dates = keys.map(key => key.split(':')[1]).sort();

  for (let i = 0; i < dates.length - 1; i++) {
    const currentDate = dates[i];
    const nextDate = dates[i + 1];

    const intersection = await redis.sinter(`login_users:${currentDate}`, `login_users:${nextDate}`);
    const currentDayUserCount = await redis.scard(`login_users:${currentDate}`);

    rr1[currentDate] = currentDayUserCount > 0 ? (intersection.length / currentDayUserCount) * 100 : 0;
  }

  return rr1;
}

async function main() {
  try {
    // Xử lý logs file
    await processLogsEfficiently('log_login.json', 'login', 'nru', 'user_id');
    await processLogsEfficiently('log_open_app.json', 'open_app', 'nrd', 'device_id');
    await processLoginForRR1Efficiently('log_login.json');

    console.log("--- Kết quả ---");

    // Lấy và hiện NRU theo ngày
    console.log("\nNRU (New Registered User) theo ngày:");
    const nruKeys = await redis.keys('nru:*');
    for (const key of nruKeys) {
      const date = key.split(':')[1];
      const count = await getHLLCount(key);
      console.log(`${date}: ~${count}`);
    }

    // Lấy và hiện NRD theo ngày
    console.log("\nNRD (New Registered Device) theo ngày:");
    const nrdKeys = await redis.keys('nrd:*');
    for (const key of nrdKeys) {
      const date = key.split(':')[1];
      const count = await getHLLCount(key);
      console.log(`${date}: ~${count}`);
    }

    // Tính toán và hiện RR1
    const rr1Result = await calculateRR1Efficiently();
    console.log("\nRR1 (Retention Rate):");
    for (const date in rr1Result) {
      console.log(`${date}: ${rr1Result[date].toFixed(2)}%`);
    }
  } catch (error) {
    console.error('Lỗi:', error);
  } finally {
    redis.quit();
  }
}

main();