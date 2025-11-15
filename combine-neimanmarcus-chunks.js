/** @format */

// Neiman Marcus Chunk Combiner
// Combines all chunk files from Downloads folder into a single catalog.json file
require("dotenv").config();
const fs = require("fs");
const zlib = require("zlib");
const path = require("path");

// Configuration - Update this path if your chunks are in a different location
const DOWNLOADS_FOLDER =
  process.env.DOWNLOADS_PATH ||
  path.join(process.env.USERPROFILE || process.env.HOME || "", "Downloads");
const OUTPUT_COUNTRY = "US";
const STORE_NAME = "Neiman Marcus";
const STORE_DOMAIN = "neimanmarcus.com";

/**
 * Find all chunk files in the Downloads folder
 */
function findChunkFiles(downloadsPath) {
  const files = fs.readdirSync(downloadsPath);
  const chunkFiles = files
    .filter((file) => file.startsWith("chunk-") && file.endsWith(".json"))
    .map((file) => ({
      filename: file,
      path: path.join(downloadsPath, file),
      chunkNumber: parseInt(file.match(/chunk-(\d+)\.json/)?.[1] || "0", 10),
    }))
    .filter((file) => file.chunkNumber > 0)
    .sort((a, b) => a.chunkNumber - b.chunkNumber);

  return chunkFiles;
}

/**
 * Load and parse a chunk file
 */
function loadChunkFile(chunkPath) {
  try {
    console.log(`📖 Reading ${path.basename(chunkPath)}...`);
    const content = fs.readFileSync(chunkPath, "utf8");
    const data = JSON.parse(content);

    // Handle both array format and object with products array
    if (Array.isArray(data)) {
      return data;
    } else if (data.products && Array.isArray(data.products)) {
      return data.products;
    } else {
      console.warn(`⚠️  Unexpected format in ${chunkPath}, skipping...`);
      return [];
    }
  } catch (error) {
    console.error(`❌ Error reading ${chunkPath}:`, error.message);
    return [];
  }
}

/**
 * Extract unique categories from products
 */
function extractCategories(products) {
  const categories = new Set();
  products.forEach((product) => {
    if (product.category) {
      categories.add(product.category);
    }
  });
  return Array.from(categories).sort();
}

/**
 * Generate combined catalog files
 */
async function generateCombinedCatalog(allProducts, storeData) {
  const countryCode = storeData.country || OUTPUT_COUNTRY;
  const cleanBrandName = "neimanmarcus";

  // Create directory structure: countryCode/retailername-countryCode/
  const dirPath = path.join(
    __dirname,
    "output",
    countryCode,
    `${cleanBrandName}-${countryCode}`
  );

  if (!fs.existsSync(dirPath)) {
    fs.mkdirSync(dirPath, { recursive: true });
    console.log(`📁 Created directory: ${dirPath}`);
  }

  // Extract categories
  const categories = extractCategories(allProducts);

  // Save formatted data as JSON
  const jsonFilePath = path.join(dirPath, "catalog.json");
  const catalogData = {
    store_info: {
      name: storeData.name || STORE_NAME,
      domain: storeData.domain || STORE_DOMAIN,
      currency: storeData.currency || "USD",
      country: countryCode,
      total_products: allProducts.length,
      categories: categories,
      crawled_at: new Date().toISOString(),
    },
    products: allProducts,
  };

  fs.writeFileSync(jsonFilePath, JSON.stringify(catalogData, null, 2), "utf8");
  console.log(`✅ JSON file generated: ${jsonFilePath}`);
  console.log(`   Total products: ${allProducts.length}`);
  console.log(`   Categories: ${categories.join(", ")}`);

  // Create JSONL file (each product on a separate line)
  const jsonlFilePath = path.join(dirPath, "catalog.jsonl");
  const jsonlContent = allProducts
    .map((product) => JSON.stringify(product))
    .join("\n");
  fs.writeFileSync(jsonlFilePath, jsonlContent, "utf8");
  console.log(`✅ JSONL file generated: ${jsonlFilePath}`);

  // Gzip the JSONL file
  const gzippedFilePath = `${jsonlFilePath}.gz`;
  const jsonlBuffer = fs.readFileSync(jsonlFilePath);
  const gzippedBuffer = zlib.gzipSync(jsonlBuffer);
  fs.writeFileSync(gzippedFilePath, gzippedBuffer);
  console.log(`✅ Gzipped JSONL file generated: ${gzippedFilePath}`);

  // Calculate file sizes
  const jsonSize = fs.statSync(jsonFilePath).size;
  const jsonlSize = fs.statSync(jsonlFilePath).size;
  const gzipSize = fs.statSync(gzippedFilePath).size;

  console.log(`\n📊 File Sizes:`);
  console.log(`   catalog.json: ${(jsonSize / 1024 / 1024).toFixed(2)} MB`);
  console.log(`   catalog.jsonl: ${(jsonlSize / 1024 / 1024).toFixed(2)} MB`);
  console.log(
    `   catalog.jsonl.gz: ${(gzipSize / 1024 / 1024).toFixed(2)} MB (${(
      (1 - gzipSize / jsonlSize) *
      100
    ).toFixed(1)}% compression)`
  );

  return {
    jsonPath: jsonFilePath,
    jsonlPath: jsonlFilePath,
    gzipPath: gzippedFilePath,
    totalProducts: allProducts.length,
    categories: categories,
  };
}

/**
 * Main function to combine all chunks
 */
async function combineChunks() {
  try {
    console.log("🚀 Starting Neiman Marcus Chunk Combiner...\n");

    // Verify Downloads folder exists
    if (!fs.existsSync(DOWNLOADS_FOLDER)) {
      console.error(
        `❌ Downloads folder not found: ${DOWNLOADS_FOLDER}\n` +
          `   Please update DOWNLOADS_FOLDER in the script or set DOWNLOADS_PATH environment variable`
      );
      return false;
    }

    // Find all chunk files
    console.log(`📂 Looking for chunk files in: ${DOWNLOADS_FOLDER}`);
    const chunkFiles = findChunkFiles(DOWNLOADS_FOLDER);

    if (chunkFiles.length === 0) {
      console.error(
        `❌ No chunk files found in ${DOWNLOADS_FOLDER}\n` +
          `   Expected files: chunk-1.json, chunk-2.json, etc.`
      );
      return false;
    }

    console.log(`✅ Found ${chunkFiles.length} chunk files\n`);

    // Load all chunks
    let allProducts = [];
    let totalVariants = 0;
    const chunkStats = [];

    for (const chunkFile of chunkFiles) {
      const products = loadChunkFile(chunkFile.path);
      const chunkVariants = products.reduce(
        (sum, p) => sum + (p.variants?.length || 0),
        0
      );

      allProducts = allProducts.concat(products);
      totalVariants += chunkVariants;

      chunkStats.push({
        chunk: chunkFile.chunkNumber,
        products: products.length,
        variants: chunkVariants,
      });

      console.log(
        `   Chunk ${chunkFile.chunkNumber}: ${products.length} products, ${chunkVariants} variants`
      );
    }

    console.log(`\n📊 Summary:`);
    console.log(`   Total chunks processed: ${chunkFiles.length}`);
    console.log(`   Total products: ${allProducts.length}`);
    console.log(`   Total variants: ${totalVariants}`);

    // Show chunk breakdown
    console.log(`\n📦 Chunk Breakdown:`);
    chunkStats.forEach((stat) => {
      console.log(
        `   Chunk ${stat.chunk}: ${stat.products} products, ${stat.variants} variants`
      );
    });

    // Generate combined catalog
    console.log(`\n${"=".repeat(60)}`);
    console.log("📚 GENERATING COMBINED CATALOG FILES");
    console.log(`${"=".repeat(60)}\n`);

    const storeData = {
      name: STORE_NAME,
      domain: STORE_DOMAIN,
      currency: "USD",
      country: OUTPUT_COUNTRY,
    };

    const result = await generateCombinedCatalog(allProducts, storeData);

    console.log(`\n${"🎉".repeat(20)}`);
    console.log("🎉 ALL CHUNKS COMBINED SUCCESSFULLY! 🎉");
    console.log(`${"🎉".repeat(20)}`);

    console.log(`\n📊 Final Results:`);
    console.log(`   Total Products: ${result.totalProducts}`);
    console.log(`   Total Variants: ${totalVariants}`);
    console.log(`   Categories: ${result.categories.length}`);
    console.log(`   Output JSON: ${result.jsonPath}`);
    console.log(`   Output JSONL: ${result.jsonlPath}`);
    console.log(`   Output GZIP: ${result.gzipPath}`);

    return true;
  } catch (error) {
    console.error("❌ Error combining chunks:", error);
    throw error;
  }
}

// Run the combiner
if (require.main === module) {
  combineChunks()
    .then((success) => {
      if (success) {
        console.log("\n✅ Script completed successfully");
        process.exit(0);
      } else {
        console.log("\n❌ Script failed");
        process.exit(1);
      }
    })
    .catch((error) => {
      console.error("❌ Script failed:", error);
      process.exit(1);
    });
}

module.exports = { combineChunks };
