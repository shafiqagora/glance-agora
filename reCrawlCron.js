/** @format */

require("dotenv").config();
const axios = require("axios");
const cron = require("node-cron");
const { processShopifyRecrawler } = require("./upload-catalogs");
const {
  processGoodAmericanRecrawlerAndUpload,
  processEverlaneRecrawlerAndUpload,
} = require("./upload-catalogs");

async function sendMessageToSlack(message) {
  const slackToken = process.env.SLACK_TOKEN; // Use environment variable
  const url = "https://slack.com/api/chat.postMessage";

  let channel = process.env.SLACK_CHANNEL || "C0951E9FAKU";

  try {
    const response = await axios.post(
      url,
      {
        channel: channel,
        text: message,
      },
      {
        headers: {
          Authorization: `Bearer ${slackToken}`,
          "Content-Type": "application/json",
        },
      }
    );

    if (response.data.ok) {
      console.log("Message sent successfully");
    } else {
      console.error("Error sending message:", response.data);
    }
  } catch (error) {
    console.error("Error sending message:");
  }
}

async function runRecrawlProcess() {
  try {
    console.log("🔄 Starting scheduled Shopify recrawl process...");

    // Send start notification
    await sendMessageToSlack(
      "🔄 Starting scheduled Shopify recrawl process at 12 PM IST..."
    );

    // Run the recrawl process
    const results = await processShopifyRecrawler();
    // await sendMessageToSlack('🔄 Starting everlane and good american recrawl')

    // Prepare success message
    let message = "✅ Shopify recrawl process completed successfully!\n\n";
    message += `📊 Summary:\n`;
    message += `• Stores recrawled: ${results.summary.stores_recrawled}\n`;
    message += `• Stores uploaded: ${results.summary.stores_uploaded}\n`;
    message += `• Upload failures: ${results.summary.upload_failed}\n`;
    message += `• Recrawl failures: ${results.summary.recrawl_failed}\n`;
    message += `• Recrawl skipped: ${results.summary.recrawl_skipped}\n`;
    message += `• Completed at: ${results.summary.completed_at}`;

    await sendMessageToSlack(message);

    // const results2 = await processGoodAmericanRecrawlerAndUpload()
    // const results3 = await processEverlaneRecrawlerAndUpload()

    // let message2 =
    //   '✅ Everlane and good american recrawl process completed successfully!\n\n'

    // await sendMessageToSlack(message2)
  } catch (error) {
    console.error("❌ Scheduled recrawl process failed:", error.message);

    // Send error notification
    const errorMessage = `❌ Shopify recrawl process failed!\n\nError: ${
      error.message
    }\nTime: ${new Date().toISOString()}`;
    await sendMessageToSlack(errorMessage);
  }
}

// Schedule the recrawl process to run daily at 12:00 PM IST
// IST is UTC+5:30, so 12:00 PM IST = 6:30 AM UTC
// Server is running in UTC, so we schedule for 6:30 AM UTC
cron.schedule(
  "30 4 * * *",
  async () => {
    console.log(
      "⏰ Running scheduled recrawl process at 10:00 PM IST (4:30 AM UTC)"
    );
    await runRecrawlProcess();
  },
  {
    scheduled: true,
  }
);

console.log(
  "🕐 Cron job scheduled: Shopify recrawl will run daily at 10:00 PM IST"
);

// console.log('running recrawl now')
// runRecrawlProcess()

// Keep the process running
process.on("SIGINT", () => {
  console.log("👋 Shutting down cron job...");
  process.exit(0);
});
