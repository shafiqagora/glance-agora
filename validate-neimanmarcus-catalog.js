/** @format */
/**
 * Validate Neiman Marcus catalog.json file
 * Uses filterValidProducts from validate-catalog.js
 */

const fs = require("fs");
const path = require("path");
const zlib = require("zlib");
const { filterValidProducts } = require("./validate-catalog");

const catalogPath = path.join(
  __dirname,
  "output",
  "US",
  "neimanmarcus-US",
  "catalog.json"
);

async function validateNeimanMarcusCatalog() {
  console.log("=".repeat(80));
  console.log("🔍 VALIDATING NEIMAN MARCUS CATALOG");
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

    // Calculate total variants
    const totalVariants = catalog.products.reduce(
      (sum, p) => sum + (p.variants?.length || 0),
      0
    );
    const validVariants = validationResult.validProducts.reduce(
      (sum, p) => sum + (p.variants?.length || 0),
      0
    );
    const invalidVariants = totalVariants - validVariants;

    console.log(`\n🔢 Total Variants: ${totalVariants}`);
    console.log(`✅ Valid Variants: ${validVariants}`);
    console.log(`❌ Invalid Variants: ${invalidVariants}`);
    console.log(
      `📈 Variant Success Rate: ${
        totalVariants > 0
          ? ((validVariants / totalVariants) * 100).toFixed(2)
          : 0
      }%`
    );

    console.log("=".repeat(80));

    // Check if user wants to save cleaned catalog
    const saveCleaned =
      process.argv.includes("--save-cleaned") || process.argv.includes("-s");

    // Save cleaned catalog if there are invalid products or if --save-cleaned flag is used
    if (validationResult.validCount < validationResult.totalCount) {
      console.log(
        `\n⚠️  ${validationResult.invalidCount} products failed validation`
      );

      if (saveCleaned) {
        console.log("\n💾 Saving cleaned catalog with only valid products...");

        const dirPath = path.dirname(catalogPath);
        const cleanedCatalogPath = path.join(dirPath, "catalog.validated.json");
        const cleanedJsonlPath = path.join(dirPath, "catalog.validated.jsonl");

        // Create cleaned catalog with store_info
        const cleanedCatalog = {
          store_info: {
            ...catalog.store_info,
            total_products: validationResult.validCount,
            validated_at: new Date().toISOString(),
            original_total_products: validationResult.totalCount,
            invalid_products_removed: validationResult.invalidCount,
          },
          products: validationResult.validProducts,
        };

        // Save JSON
        fs.writeFileSync(
          cleanedCatalogPath,
          JSON.stringify(cleanedCatalog, null, 2),
          "utf8"
        );
        console.log(`✅ Cleaned JSON saved: ${cleanedCatalogPath}`);

        // Save JSONL
        const jsonlContent = validationResult.validProducts
          .map((product) => JSON.stringify(product))
          .join("\n");
        fs.writeFileSync(cleanedJsonlPath, jsonlContent, "utf8");
        console.log(`✅ Cleaned JSONL saved: ${cleanedJsonlPath}`);

        // Gzip JSONL
        const gzippedPath = `${cleanedJsonlPath}.gz`;
        const jsonlBuffer = fs.readFileSync(cleanedJsonlPath);
        const gzippedBuffer = zlib.gzipSync(jsonlBuffer);
        fs.writeFileSync(gzippedPath, gzippedBuffer);
        console.log(`✅ Gzipped JSONL saved: ${gzippedPath}`);

        const jsonSize = fs.statSync(cleanedCatalogPath).size;
        const jsonlSize = fs.statSync(cleanedJsonlPath).size;
        const gzipSize = fs.statSync(gzippedPath).size;

        console.log(`\n📊 Cleaned Catalog Sizes:`);
        console.log(`   JSON: ${(jsonSize / 1024 / 1024).toFixed(2)} MB`);
        console.log(`   JSONL: ${(jsonlSize / 1024 / 1024).toFixed(2)} MB`);
        console.log(
          `   GZIP: ${(gzipSize / 1024 / 1024).toFixed(2)} MB (${(
            (1 - gzipSize / jsonlSize) *
            100
          ).toFixed(1)}% compression)`
        );
      } else {
        console.log(
          `\n💡 Tip: Run with --save-cleaned flag to save a cleaned catalog with only valid products.`
        );
      }

      process.exit(1);
    } else {
      console.log("\n✅ ALL PRODUCTS ARE VALID!");
      console.log("✅ No MPN inconsistencies detected!");

      // Save cleaned catalog even if all products are valid (to fix structure)
      if (saveCleaned) {
        console.log("\n💾 Saving cleaned catalog with corrected structure...");

        const dirPath = path.dirname(catalogPath);
        const cleanedCatalogPath = path.join(dirPath, "catalog.validated.json");
        const cleanedJsonlPath = path.join(dirPath, "catalog.validated.jsonl");

        // Create cleaned catalog with store_info
        const cleanedCatalog = {
          store_info: {
            ...catalog.store_info,
            total_products: validationResult.validCount,
            validated_at: new Date().toISOString(),
            original_total_products: validationResult.totalCount,
            invalid_products_removed: validationResult.invalidCount,
          },
          products: validationResult.validProducts,
        };

        // Save JSON
        fs.writeFileSync(
          cleanedCatalogPath,
          JSON.stringify(cleanedCatalog, null, 2),
          "utf8"
        );
        console.log(`✅ Cleaned JSON saved: ${cleanedCatalogPath}`);

        // Save JSONL
        const jsonlContent = validationResult.validProducts
          .map((product) => JSON.stringify(product))
          .join("\n");
        fs.writeFileSync(cleanedJsonlPath, jsonlContent, "utf8");
        console.log(`✅ Cleaned JSONL saved: ${cleanedJsonlPath}`);

        // Gzip JSONL
        const gzippedPath = `${cleanedJsonlPath}.gz`;
        const jsonlBuffer = fs.readFileSync(cleanedJsonlPath);
        const gzippedBuffer = zlib.gzipSync(jsonlBuffer);
        fs.writeFileSync(gzippedPath, gzippedBuffer);
        console.log(`✅ Gzipped JSONL saved: ${gzippedPath}`);

        const jsonSize = fs.statSync(cleanedCatalogPath).size;
        const jsonlSize = fs.statSync(cleanedJsonlPath).size;
        const gzipSize = fs.statSync(gzippedPath).size;

        console.log(`\n📊 Cleaned Catalog Sizes:`);
        console.log(`   JSON: ${(jsonSize / 1024 / 1024).toFixed(2)} MB`);
        console.log(`   JSONL: ${(jsonlSize / 1024 / 1024).toFixed(2)} MB`);
        console.log(
          `   GZIP: ${(gzipSize / 1024 / 1024).toFixed(2)} MB (${(
            (1 - gzipSize / jsonlSize) *
            100
          ).toFixed(1)}% compression)`
        );
      }

      process.exit(0);
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
  validateNeimanMarcusCatalog().catch((error) => {
    console.error("💥 Unexpected error:", error);
    process.exit(1);
  });
}

module.exports = { validateNeimanMarcusCatalog };
