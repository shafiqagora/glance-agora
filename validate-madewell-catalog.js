/** @format */
/**
 * Validate Madewell catalog.json file
 * Uses filterValidProducts from validate-catalog.js
 */

const fs = require("fs");
const path = require("path");
const { filterValidProducts } = require("./validate-catalog");

const catalogPath = path.join(
  __dirname,
  "output",
  "US",
  "madewell-US",
  "catalog.json"
);

async function validateMadewellCatalog() {
  console.log("=".repeat(80));
  console.log("🔍 VALIDATING MADEWELL CATALOG");
  console.log("=".repeat(80));
  console.log(`📁 Catalog: ${catalogPath}\n`);

  if (!fs.existsSync(catalogPath)) {
    console.error(`❌ Catalog file not found: ${catalogPath}`);
    process.exit(1);
  }

  try {
    // Read catalog
    console.log("📖 Reading catalog file...");
    const catalogContent = fs.readFileSync(catalogPath, "utf8");
    const catalog = JSON.parse(catalogContent);

    if (!catalog.products || !Array.isArray(catalog.products)) {
      console.error("❌ Invalid catalog format: missing products array");
      process.exit(1);
    }

    console.log(`📦 Found ${catalog.products.length} products in catalog\n`);

    // Validate using filterValidProducts
    console.log("🔍 Validating products...\n");
    const validationResult = filterValidProducts(catalog.products);

    // Print results
    console.log("=".repeat(80));
    console.log("📊 VALIDATION RESULTS");
    console.log("=".repeat(80));
    console.log(`📦 Total Products: ${validationResult.totalCount}`);
    console.log(`✅ Valid Products: ${validationResult.validCount}`);
    console.log(`❌ Invalid Products: ${validationResult.invalidCount}`);
    console.log(
      `🗑️  Variants Filtered: ${validationResult.totalVariantsFiltered || 0}`
    );

    const successRate =
      validationResult.totalCount > 0
        ? (
            (validationResult.validCount / validationResult.totalCount) *
            100
          ).toFixed(2)
        : 0;
    console.log(`📈 Success Rate: ${successRate}%`);

    console.log("=".repeat(80));

    if (validationResult.validCount === validationResult.totalCount) {
      console.log("\n✅ ALL PRODUCTS ARE VALID!");
      console.log("✅ No MPN inconsistencies detected!");
      process.exit(0);
    } else {
      console.log(
        `\n⚠️  ${validationResult.invalidCount} products failed validation`
      );
      process.exit(1);
    }
  } catch (error) {
    console.error("\n❌ Validation failed with error:");
    console.error(error.message);
    if (error.stack) {
      console.error("\nStack trace:");
      console.error(error.stack);
    }
    process.exit(1);
  }
}

// Run validation
if (require.main === module) {
  validateMadewellCatalog().catch((error) => {
    console.error("💥 Unexpected error:", error);
    process.exit(1);
  });
}

module.exports = { validateMadewellCatalog };
