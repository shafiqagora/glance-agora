#!/usr/bin/env node

const fs = require('fs')
const path = require('path')
const readline = require('readline')

// Product-level mandatory fields
const PRODUCT_MANDATORY_FIELDS = [
  'name', // Product name
  'description', // Description (will be checked for more than 1 word)
]

// Variant-level mandatory fields
const VARIANT_MANDATORY_FIELDS = [
  'original_price', // Price (will be checked as numeric and not 0)
  'color', // Color
  'size', // Size
  'variant_id', // Variant ID
  'image_url', // Main image URL
  'alternate_image_urls', // Alt Image URLs
  'link_url', // Link URL
  'deeplink_url', // Deeplink URL
  'parent_product_id', // Parent Product ID
  'is_in_stock', // Is in stock
  'is_on_sale', // Is on Sale
  'mpn', // MPN
]

class CatalogValidator {
  constructor() {
    this.errors = []
    this.warnings = []
    this.stats = {
      totalProducts: 0,
      totalVariants: 0,
      validProducts: 0,
      invalidProducts: 0,
      validVariants: 0,
      invalidVariants: 0,
      filteredVariants: 0,
      productsWithAllVariantsRemoved: 0,
    }
    this.productIds = new Set()
    this.variantIds = new Set()
    this.mpnsByProduct = new Map() // parent_product_id -> Map(color -> Set(mpn))
  }

  log(message, type = 'info') {
    const timestamp = new Date().toISOString()
    const prefix = type === 'error' ? '❌' : type === 'warning' ? '⚠️' : 'ℹ️'
    console.log(`${prefix} [${timestamp}] ${message}`)
  }

  addError(message) {
    this.errors.push(message)
    this.log(message, 'error')
  }

  addWarning(message) {
    this.warnings.push(message)
    this.log(message, 'warning')
  }

  // Check if all mandatory product-level fields are present
  validateProductMandatoryFields(product, lineNumber) {
    const missingFields = []

    for (const field of PRODUCT_MANDATORY_FIELDS) {
      if (
        !(field in product) ||
        product[field] === null ||
        product[field] === undefined ||
        product[field] === ''
      ) {
        missingFields.push(field)
      }
    }

    if (missingFields.length > 0) {
      this.addError(
        `Line ${lineNumber}: Missing mandatory product fields: ${missingFields.join(
          ', '
        )}`
      )
      return false
    }
    return true
  }

  // Check if all mandatory variant-level fields are present
  validateVariantMandatoryFields(variant, lineNumber) {
    const missingFields = []

    for (const field of VARIANT_MANDATORY_FIELDS) {
      if (
        !(field in variant) ||
        variant[field] === null ||
        variant[field] === undefined ||
        variant[field] === ''
      ) {
        missingFields.push(field)
      }
    }

    if (missingFields.length > 0) {
      this.addError(
        `Line ${lineNumber}: Missing mandatory variant fields: ${missingFields.join(
          ', '
        )}`
      )
      return false
    }
    return true
  }

  // Check if price is numeric and not 0
  validatePrice(variant, lineNumber) {
    const price = variant.original_price

    if (typeof price !== 'number' && typeof price !== 'string') {
      this.addError(
        `Line ${lineNumber}: Price must be numeric, got ${typeof price}`
      )
      return false
    }

    const numericPrice = typeof price === 'string' ? parseFloat(price) : price

    if (isNaN(numericPrice)) {
      this.addError(`Line ${lineNumber}: Price is not a valid number: ${price}`)
      return false
    }

    if (numericPrice <= 0) {
      this.addError(
        `Line ${lineNumber}: Price must be greater than 0, got ${numericPrice}`
      )
      return false
    }

    return true
  }

  // Check if description has more than 1 word
  validateDescription(product, lineNumber) {
    const description = product.description

    if (typeof description !== 'string') {
      this.addError(
        `Line ${lineNumber}: Description must be a string, got ${typeof description}`
      )
      return false
    }

    const words = description
      .trim()
      .split(/\s+/)
      .filter((word) => word.length > 0)

    if (words.length <= 1) {
      this.addError(
        `Line ${lineNumber}: Description must contain more than 1 word, got: "${description}"`
      )
      return false
    }

    return true
  }

  // Check variant ID uniqueness
  validateVariantIdUniqueness(variant, lineNumber) {
    const productId = variant.parent_product_id
    const variantId = variant.variant_id

    // Check variant ID uniqueness (should always be unique)
    if (this.variantIds.has(variantId)) {
      this.addError(
        `Line ${lineNumber}: Duplicate variant ID found: ${variantId}`
      )
      return false
    }
    this.variantIds.add(variantId)

    // Track product IDs for MPN grouping validation
    if (!this.mpnsByProduct.has(productId)) {
      this.mpnsByProduct.set(productId, new Map())
    }

    return true
  }

  // Validate MPN grouping by color within products
  validateMpnGrouping(variant, lineNumber) {
    const productId = variant.parent_product_id
    const color = variant.color
    const mpn = variant.mpn

    const productMpns = this.mpnsByProduct.get(productId)

    if (!productMpns.has(color)) {
      productMpns.set(color, new Set())
    }

    const colorMpns = productMpns.get(color)

    // Check if this MPN already exists for this color in this product
    if (colorMpns.has(mpn)) {
      this.addWarning(
        `Line ${lineNumber}: Duplicate MPN "${mpn}" found for color "${color}" in product "${productId}"`
      )
    }

    colorMpns.add(mpn)
    return true
  }

  // Validate a single variant
  validateVariant(variant, lineNumber) {
    this.stats.totalVariants++

    let isValid = true

    // 1. Check mandatory variant fields
    if (!this.validateVariantMandatoryFields(variant, lineNumber)) {
      isValid = false
    }

    // 2. Check price validation
    if (!this.validatePrice(variant, lineNumber)) {
      isValid = false
    }

    // 3. Check variant ID uniqueness
    if (!this.validateVariantIdUniqueness(variant, lineNumber)) {
      isValid = false
    }

    // 4. Validate MPN grouping
    this.validateMpnGrouping(variant, lineNumber)

    if (isValid) {
      this.stats.validVariants++
    } else {
      this.stats.invalidVariants++
    }

    return isValid
  }

  // Validate a complete product with its variants
  validateProduct(product, lineNumber) {
    this.stats.totalProducts++

    let isProductValid = true

    // 1. Check mandatory product fields
    if (!this.validateProductMandatoryFields(product, lineNumber)) {
      isProductValid = false
    }

    // 2. Check description validation
    if (!this.validateDescription(product, lineNumber)) {
      isProductValid = false
    }

    // If product-level validation fails, mark entire product as invalid
    if (!isProductValid) {
      this.stats.invalidProducts++
      return { isValid: false, validVariants: [] }
    }

    // Validate variants and filter out invalid ones
    const validVariants = []
    if (product.variants && Array.isArray(product.variants)) {
      for (let i = 0; i < product.variants.length; i++) {
        const variant = product.variants[i]

        // Inherit product-level fields if missing in variant
        const enrichedVariant = {
          name: product.name || variant.name,
          description: product.description || variant.description,
          parent_product_id:
            product.parent_product_id || variant.parent_product_id,
          ...variant,
        }

        if (this.validateVariant(enrichedVariant, `${lineNumber}.${i + 1}`)) {
          validVariants.push(enrichedVariant)
        } else {
          this.stats.filteredVariants++
        }
      }
    }

    // Check if product has any valid variants left
    if (validVariants.length === 0) {
      this.stats.productsWithAllVariantsRemoved++
      this.stats.invalidProducts++
      return { isValid: false, validVariants: [] }
    }

    this.stats.validProducts++
    return { isValid: true, validVariants }
  }

  // Handle different data structures (flat variants vs nested structure)
  processJsonLine(line, lineNumber) {
    try {
      const data = JSON.parse(line)

      // Check if this is a nested structure (has variants array)
      if (data.variants && Array.isArray(data.variants)) {
        // Validate the complete product with variants
        const result = this.validateProduct(data, lineNumber)

        // Return the filtered product data if valid
        if (result.isValid) {
          return {
            isValid: true,
            filteredProduct: {
              ...data,
              variants: result.validVariants,
            },
          }
        } else {
          return { isValid: false, filteredProduct: null }
        }
      } else {
        // Flat structure - each line is a variant, treat as single-variant product
        const singleVariantProduct = {
          name: data.name,
          description: data.description,
          parent_product_id: data.parent_product_id,
          category: data.category,
          retailer_domain: data.retailer_domain,
          brand: data.brand,
          gender: data.gender,
          materials: data.materials,
          return_policy_link: data.return_policy_link,
          return_policy: data.return_policy,
          size_chart: data.size_chart,
          available_bank_offers: data.available_bank_offers,
          available_coupons: data.available_coupons,
          operation_type: data.operation_type,
          source: data.source,
          variants: [data],
        }

        const result = this.validateProduct(singleVariantProduct, lineNumber)

        if (result.isValid) {
          return {
            isValid: true,
            filteredProduct: {
              ...singleVariantProduct,
              variants: result.validVariants,
            },
          }
        } else {
          return { isValid: false, filteredProduct: null }
        }
      }
    } catch (error) {
      this.addError(
        `Line ${lineNumber}: Invalid JSON syntax - ${error.message}`
      )
      return { isValid: false, filteredProduct: null }
    }
  }

  // Validate JSONL file and return filtered products
  async validateFile(filePath) {
    this.log(`Validating file: ${filePath}`)

    if (!fs.existsSync(filePath)) {
      this.addError(`File not found: ${filePath}`)
      return { isValid: false, filteredProducts: [] }
    }

    const fileStream = fs.createReadStream(filePath)
    const rl = readline.createInterface({
      input: fileStream,
      crlfDelay: Infinity,
    })

    let lineNumber = 0
    let hasValidJson = false
    const filteredProducts = []

    for await (const line of rl) {
      lineNumber++

      // Skip empty lines
      if (line.trim() === '') {
        continue
      }

      const result = this.processJsonLine(line, lineNumber)
      if (result && result.isValid) {
        hasValidJson = true
        if (result.filteredProduct) {
          filteredProducts.push(result.filteredProduct)
        }
      }
    }

    // Check if file had any valid JSON
    if (!hasValidJson && lineNumber > 0) {
      this.addError(`File contains no valid JSON lines: ${filePath}`)
    }

    return { isValid: hasValidJson, filteredProducts }
  }

  // Final validation checks after processing all data
  performFinalValidation() {
    this.log('Performing final validation checks...')

    // Check for MPN consistency within products and colors
    for (const [productId, colorMpns] of this.mpnsByProduct.entries()) {
      for (const [color, mpns] of colorMpns.entries()) {
        if (mpns.size > 1) {
          this.addWarning(
            `Product "${productId}" has multiple MPNs for color "${color}": ${Array.from(
              mpns
            ).join(', ')}`
          )
        }
      }
    }
  }

  // Print validation summary
  printSummary() {
    console.log('\n' + '='.repeat(60))
    console.log('📊 VALIDATION SUMMARY')
    console.log('='.repeat(60))

    console.log(`📦 Total Products: ${this.stats.totalProducts}`)
    console.log(`✅ Valid Products: ${this.stats.validProducts}`)
    console.log(`❌ Invalid Products: ${this.stats.invalidProducts}`)
    console.log(
      `🗑️ Products with All Variants Removed: ${this.stats.productsWithAllVariantsRemoved}`
    )

    console.log(`\n🔢 Total Variants: ${this.stats.totalVariants}`)
    console.log(`✅ Valid Variants: ${this.stats.validVariants}`)
    console.log(`❌ Invalid Variants: ${this.stats.invalidVariants}`)
    console.log(`🗑️ Filtered Out Variants: ${this.stats.filteredVariants}`)

    console.log(`\n🆔 Unique Variant IDs: ${this.variantIds.size}`)
    console.log(`🏷️ Unique Product IDs: ${this.mpnsByProduct.size}`)

    console.log(`\n🚨 Total Errors: ${this.errors.length}`)
    console.log(`⚠️ Total Warnings: ${this.warnings.length}`)

    const productSuccessRate =
      this.stats.totalProducts > 0
        ? ((this.stats.validProducts / this.stats.totalProducts) * 100).toFixed(
            2
          )
        : 0
    const variantSuccessRate =
      this.stats.totalVariants > 0
        ? ((this.stats.validVariants / this.stats.totalVariants) * 100).toFixed(
            2
          )
        : 0

    console.log(`📈 Product Success Rate: ${productSuccessRate}%`)
    console.log(`📈 Variant Success Rate: ${variantSuccessRate}%`)

    if (this.errors.length > 0) {
      console.log('\n❌ CRITICAL ERRORS FOUND:')
      console.log('The following issues must be fixed:')
      this.errors.slice(0, 10).forEach((error, index) => {
        console.log(`  ${index + 1}. ${error}`)
      })

      if (this.errors.length > 10) {
        console.log(`  ... and ${this.errors.length - 10} more errors`)
      }
    }

    if (this.warnings.length > 0) {
      console.log('\n⚠️ WARNINGS:')
      this.warnings.slice(0, 5).forEach((warning, index) => {
        console.log(`  ${index + 1}. ${warning}`)
      })

      if (this.warnings.length > 5) {
        console.log(`  ... and ${this.warnings.length - 5} more warnings`)
      }
    }

    console.log('\n' + '='.repeat(60))

    return {
      isValid: this.errors.length === 0,
      errors: this.errors.length,
      warnings: this.warnings.length,
      stats: this.stats,
    }
  }
}

// Main function
async function main() {
  const args = process.argv.slice(2)

  if (args.length === 0) {
    console.log(
      'Usage: node validate-catalog.js <jsonl-file-path> [additional-files...]'
    )
    console.log(
      '   or: node validate-catalog.js --all  (to validate all catalog files)'
    )
    process.exit(1)
  }

  const validator = new CatalogValidator()

  try {
    let allFilteredProducts = []

    if (args[0] === '--all') {
      // Validate all catalog files in output directory
      const outputDir = path.join(__dirname, 'output')
      const regions = ['US', 'IN']

      for (const region of regions) {
        const regionDir = path.join(outputDir, region)
        if (!fs.existsSync(regionDir)) continue

        const stores = fs.readdirSync(regionDir)
        for (const store of stores) {
          const storeDir = path.join(regionDir, store)
          const catalogFile = path.join(storeDir, 'catalog.jsonl')

          if (fs.existsSync(catalogFile)) {
            const result = await validator.validateFile(catalogFile)
            if (result.filteredProducts) {
              allFilteredProducts.push(...result.filteredProducts)
            }
          }
        }
      }
    } else {
      // Validate specific files
      for (const filePath of args) {
        const result = await validator.validateFile(filePath)
        if (result.filteredProducts) {
          allFilteredProducts.push(...result.filteredProducts)
        }
      }
    }

    validator.performFinalValidation()
    const summary = validator.printSummary()

    // Log filtered products count
    console.log(
      `\n🔄 Total Filtered Products Available: ${allFilteredProducts.length}`
    )

    // Exit with error code if validation failed
    process.exit(summary.isValid ? 0 : 1)
  } catch (error) {
    console.error('❌ Validation failed with error:', error.message)
    process.exit(1)
  }
}

// Run if called directly
if (require.main === module) {
  main().catch(console.error)
}

// Function to filter products and return only valid ones with counts
function filterValidProducts(products) {
  const validator = new CatalogValidator()
  const validProducts = []
  let invalidCount = 0

  for (let i = 0; i < products.length; i++) {
    const product = products[i]

    // Create a temporary validator to check this product
    const tempValidator = new CatalogValidator()
    const result = tempValidator.validateProduct(product, i + 1)

    if (result.isValid && result.validVariants.length > 0) {
      // Check MPN consistency within same color before accepting the product
      const colorMpnMap = new Map() // color -> Set(mpn)

      for (const variant of result.validVariants) {
        const color = variant.color
        const mpn = variant.mpn

        if (!colorMpnMap.has(color)) {
          colorMpnMap.set(color, new Set())
        }

        colorMpnMap.get(color).add(mpn)
      }

      // Check if any color has multiple MPNs
      for (const [color, mpns] of colorMpnMap.entries()) {
        if (mpns.size > 1) {
          const productId =
            product.parent_product_id ||
            product.variants?.[0]?.parent_product_id ||
            'unknown'
          const mpnList = Array.from(mpns).join(', ')
          throw new Error(
            `MPN inconsistency detected in product "${productId}": ` +
              `Variants with color "${color}" have different MPNs: [${mpnList}]. ` +
              `All variants with the same color must have the same MPN.`
          )
        }
      }

      // Return product with filtered variants
      validProducts.push({
        ...product,
        variants: result.validVariants,
      })
    } else {
      invalidCount++
    }
  }

  return {
    validProducts,
    validCount: validProducts.length,
    invalidCount,
    totalCount: products.length,
    totalVariantsFiltered: validator.stats.filteredVariants,
  }
}

module.exports = { CatalogValidator, filterValidProducts }
