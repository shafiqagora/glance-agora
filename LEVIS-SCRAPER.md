# Levi's Website Scraper

A comprehensive Puppeteer-based web scraper for extracting product information from Levi's website.

## Features

- 🎯 **Targeted Scraping**: Scrapes specific product categories (Men's and Women's clothing)
- 🔄 **Automatic Navigation**: Handles page navigation and product link extraction
- 📊 **Rich Data Extraction**: Collects comprehensive product details including:
  - Product title, price, and SKU
  - Images, colors, and sizes
  - Product descriptions and features
  - Materials and care instructions
  - Ratings and reviews
  - Category information
- 🛡️ **Anti-Bot Protection**: Includes delays, user agent spoofing, and popup handling
- 📁 **JSON Output**: Saves data in structured JSON format with timestamps
- ⚙️ **Configurable**: Easily customizable through configuration file

## Prerequisites

Before running the scraper, you need to install Puppeteer:

```bash
npm install puppeteer
```

If you encounter permission issues during installation, try:

```bash
PUPPETEER_SKIP_CHROMIUM_DOWNLOAD=true npm install puppeteer-core
```

Then ensure you have Chrome or Chromium installed on your system.

## Quick Start

1. **Basic Usage**:

   ```bash
   npm run levis:scrape
   ```

2. **Direct Node Execution**:
   ```bash
   node levis-scraper.js
   ```

## Configuration

The scraper can be customized through the `levis-config.js` file:

### Browser Settings

```javascript
browser: {
    headless: false,    // Set to true for background runs
    slowMo: 100,       // Delay between actions (ms)
    timeout: 30000     // Page load timeout (ms)
}
```

### Delay Settings

```javascript
delays: {
    pageLoad: 3000,        // Wait after page load
    betweenProducts: 1000, // Wait between products
    randomExtra: 2000,     // Additional random delay
    betweenPages: 3000     // Wait between pages
}
```

### Target URLs

```javascript
urls: [
  'https://www.levi.com/US/en_US/clothing/men/c/levi_clothing_men?page=5',
  'https://www.levi.com/US/en_US/clothing/women/c/levi_clothing_women',
]
```

## Output Files

The scraper generates two types of files:

1. **Main Data File**: `levis-products-{timestamp}.json`

   - Contains all scraped product data
   - Includes metadata and timestamps

2. **Summary File**: `levis-summary-{timestamp}.json`
   - Contains overview statistics
   - Price ranges and categories
   - Total product count

### Sample Output Structure

```json
{
  "scrapedAt": "2024-01-15T10:30:00.000Z",
  "totalProducts": 45,
  "products": [
    {
      "url": "https://www.levi.com/US/en_US/...",
      "title": "501® Original Jeans",
      "price": "$89.50",
      "originalPrice": "$119.50",
      "description": "The original blue jean since 1873...",
      "sku": "00501-2314",
      "brand": "Levi's",
      "category": "Men > Jeans",
      "images": ["https://..."],
      "colors": ["Blue", "Black"],
      "sizes": ["30x30", "32x32", "34x34"],
      "features": ["100% Cotton", "Button Fly"],
      "materials": ["Cotton"],
      "availability": "In Stock",
      "scrapedAt": "2024-01-15T10:30:15.000Z"
    }
  ]
}
```

## Customization

### Adding New URLs

Edit the `urls` array in `levis-config.js`:

```javascript
urls: [
  'https://www.levi.com/US/en_US/clothing/men/c/levi_clothing_men',
  'https://www.levi.com/US/en_US/clothing/women/c/levi_clothing_women',
  'https://www.levi.com/US/en_US/accessories/c/levi_accessories',
]
```

### Updating Selectors

If Levi's changes their website structure, update the selectors in `levis-config.js`:

```javascript
selectors: {
    title: 'h1, .product-title, [data-testid="product-title"]',
    price: '.price, .product-price, [data-testid="price"]',
    // ... other selectors
}
```

### Performance Tuning

Adjust performance settings for faster scraping:

```javascript
performance: {
    blockImages: true,      // Skip image loading
    blockStylesheets: true, // Skip CSS loading
    blockFonts: true       // Skip font loading
}
```

## Troubleshooting

### Common Issues

1. **Permission Errors**:

   - Use `PUPPETEER_SKIP_CHROMIUM_DOWNLOAD=true npm install puppeteer-core`
   - Ensure Chrome/Chromium is installed

2. **Timeout Errors**:

   - Increase timeout values in config
   - Check internet connection

3. **No Products Found**:

   - Verify URLs are accessible
   - Check if selectors need updating

4. **Bot Detection**:
   - Increase delays between requests
   - Enable headless mode: `headless: true`

### Debug Mode

For debugging, set `headless: false` in the config to see the browser in action.

## Best Practices

1. **Respectful Scraping**:

   - Don't reduce delays too much
   - Avoid running multiple instances simultaneously
   - Consider scraping during off-peak hours

2. **Data Quality**:

   - Validate output data regularly
   - Monitor for changes in website structure
   - Keep selectors updated

3. **Error Handling**:
   - Check logs for failed extractions
   - Retry failed products manually if needed

## Legal Considerations

- Ensure compliance with Levi's Terms of Service
- Respect robots.txt guidelines
- Use scraped data responsibly
- Consider rate limiting and respectful scraping practices

## Contributing

To add new features or fix issues:

1. Modify the main scraper file: `levis-scraper.js`
2. Update configuration: `levis-config.js`
3. Test with various product pages
4. Update documentation

## Support

For issues or questions:

1. Check the troubleshooting section
2. Verify configuration settings
3. Test with simpler URLs first
4. Review browser console for errors

---

**Note**: This scraper is for educational and research purposes. Always respect website terms of service and implement appropriate rate limiting.
