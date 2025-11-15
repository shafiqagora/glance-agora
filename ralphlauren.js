/** @format */

const puppeteer = require("puppeteer-extra");
const StealthPlugin = require("puppeteer-extra-plugin-stealth");
const { v4: uuidv4, v5: uuidv5 } = require("uuid");
const axios = require("axios");
const cheerio = require("cheerio");
const fs = require("fs");
const path = require("path");
const { retryRequestWithProxyRotation } = require("./utils/helper");

// Use stealth plugin to avoid detection
puppeteer.use(StealthPlugin());

// Categories to scrape
const CATEGORIES = [
  // Women's categories
  {
    url: "https://www.ralphlauren.com/women-clothing?prefn1=CategoryCode&prefv1=Sweaters",
    gender: "Female",
    category: "Sweaters",
  },
  {
    url: "https://www.ralphlauren.com/women-clothing?prefn1=CategoryCode&prefv1=Dresses",
    gender: "Female",
    category: "Dresses",
  },
  {
    url: "https://www.ralphlauren.com/women-clothing?prefn1=CategoryCode&prefv1=Jackets",
    gender: "Female",
    category: "Jackets",
  },
  {
    url: "https://www.ralphlauren.com/women-clothing?prefn1=CategoryCode&prefv1=Coats",
    gender: "Female",
    category: "Coats",
  },
  {
    url: "https://www.ralphlauren.com/women-clothing?prefn1=CategoryCode&prefv1=Blazers",
    gender: "Female",
    category: "Blazers",
  },
  {
    url: "https://www.ralphlauren.com/women-clothing?prefn1=CategoryCode&prefv1=Shirts%20%26%20Blouses",
    gender: "Female",
    category: "Shirts & Blouses",
  },
  {
    url: "https://www.ralphlauren.com/women-clothing?prefn1=CategoryCode&prefv1=Pants%7CJeans",
    gender: "Female",
    category: "Pants & Jeans",
  },
  {
    url: "https://www.ralphlauren.com/women-clothing?prefn1=CategoryCode&prefv1=T-Shirts",
    gender: "Female",
    category: "T-Shirts",
  },
  // Men's categories
  {
    url: "https://www.ralphlauren.com/men-clothing?prefn1=CategoryCode&prefv1=Jackets%2C%20Coats%20%26%20Vests%7CSport%20Coats%20%26%20Blazers",
    gender: "Male",
    category: "Sport Coats & Blazers",
  },
  {
    url: "https://www.ralphlauren.com/men-clothing?prefn1=CategoryCode&prefv1=Jackets%2C%20Coats%20%26%20Vests%7CCasual%20Shirts",
    gender: "Male",
    category: "Jackets, Coats & Casual Shirts",
  },
  {
    url: "https://www.ralphlauren.com/men-clothing?prefn1=CategoryCode&prefv1=Jackets%2C%20Coats%20%26%20Vests%7CT-Shirts",
    gender: "Male",
    category: "Jackets, Coats & T-Shirts",
  },
];

// Store details
const STORE_DETAILS = {
  retailer_domain: "ralphlauren.com",
  brand: "Ralph Lauren",
  return_policy_link: "https://www.ralphlauren.com/returns",
  return_policy:
    "Free shipping and returns on all orders. Holiday Returns Policy with extended return period during holiday season. Start a free online return or exchange anytime. Returns accepted within 30 days of purchase for regular items. Return processing includes multiple shipping options with various delivery costs. Holiday shipping available with special timing. Customized and personalized items have specific return policies. Price adjustments available on eligible items. Satisfaction guarantee and pricing guarantee on all products. Gift returns accepted with gift packaging and messaging options. In-store services and alterations available at retail locations. For complete details on shipping status, order tracking, payment methods, and international shipping, visit our customer service page.",
  size_chart: "",
  currency: "USD",
  country: "US",
};

// Function to extract product details for a specific color using AJAX endpoint
async function extractColorDetails(productId, colorName, cookieString = "") {
  try {
    const headers = {
      accept:
        "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
      "accept-language": "en-US,en;q=0.9",
      "cache-control": "max-age=0",
      cookie:
        'dwanonymous_55b6a3b329e729876c1d594e39f4ac4e=acaTqe7Av6CG5NyBk6ttYFK2cd; pzcookie="{"pz_id":"","EP_RID":""}"; _pxvid=ea996508-bd9b-11f0-88b2-baffecc75bbe; __pxvid=facd4d8e-c053-11f0-abb9-86ca80cf842b; _fbp=fb.1.1763012739255.336156945516096323; crl8.fpcuid=8c310380-6654-491b-9067-f3737cca984c; s_fid=14E4A1A092E1A9B1-0C9A602E97D9FD7F; _scid=E6K4X_ffRgRqa85UfM3NsOm3dNF_zBP0; _ScCbts=%5B%22574%3Bchrome.2%3A2%3A5%22%5D; _tt_enable_cookie=1; _ttp=01K9XVZ80GRHA6A2P4CH8X5V46_.tt.1; s_vi=[CS]v1|348AB846DD33C8A5-400002DC2134BABB[CE]; _sctr=1%7C1762974000000; mt.v=5.1698363828.1763012458976; headerDefault=1; _gid=GA1.2.485205492.1763012787; _gcl_au=1.1.1531361348.1763012792; __exponea_etc__=f26230b1-83c5-4a5b-b969-aecca0bfa66d; BVBRANDID=4315119d-cec0-43db-842b-9db4cac24baa; _pin_unauth=dWlkPVl6Y3hOV00xTm1VdFpEQm1aQzAwT1RobUxXSTFZbUV0WVdVNVl6QmtaamhqTUdJMw; _cs_c=0; rmStore=atm:pixel; kampyle_userid=6a12-4321-8c44-556e-3087-6698-c54a-1d27; _lc2_fpi=dafbb3cf7621--01k9xw1wh2xxhet6cm2xpsstd9; afUserId=5f9bd9e6-0776-449d-8099-ba2a40c65b6a-p; AF_SYNC=1763012836430; _li_ss=CgA; kndctr_F18502BE5329FB670A490D4C_AdobeOrg_identity=CiYwNzg4ODcxMjcwODIwMDMwNTQzMjU3MjQ1MDM3MjIxMzc5MjY0NFIQCMTDg96nMxgBKgNWQTYwAfABxMOD3qcz; bluecoreNV=false; AMCV_F18502BE5329FB670A490D4C%40AdobeOrg=179643557%7CMCMID%7C07888712708200305432572450372213792644%7CMCAID%7C348AB846DD33C8A5-400002DC2134BABB%7CvVersion%7C5.5.0; __cq_uuid=acRCt9DzhyGBPq83a8y7ubEXF6; mt.division=women; mt.brand=polo ralph lauren; DECLINED_DATE=1763046480182; _cdp_segments=6734e24d19ed7cc4d76f7d83%3A6736054d59c00bf81667c623; kampylePageLoadedTimestamp=1763053637737; dwac_102c95db27e6f188d36d6303ba=QfJKb0RwRsxISql8E8x0bXHRLnBk58dgDe8%3D|dw-only|||USD|false|US%2FEastern|true; cqcid=acaTqe7Av6CG5NyBk6ttYFK2cd; cquid=||; sid=QfJKb0RwRsxISql8E8x0bXHRLnBk58dgDe8; isGuestCustomer=true; __cq_dnt=0; dw_dnt=0; dwsid=yNqD9_gZQETxmG4xfRkdOvj55-jYjsHotD2pN4onp_XR2GofaXySnQhDcbN0FjIfFpJQllXr2giSJf-nke9vAw==; dw=1; dw_cookies_accepted=1; pxcts=2587f61c-c0fb-11f0-a019-736f6db0b542; s_cc=true; gender_browse_affinity=women; BVImplfy24_redesign=13693_12_0; __exponea_time2__=3.6653330326080322; _cs_cvars=%7B%7D; _li_dcdm_c=.ralphlauren.com; _lc2_fpi_js=dafbb3cf7621--01k9xw1wh2xxhet6cm2xpsstd9; _bts=764167b4-f635-4b54-d7f4-51626f04c178; forterToken=59625d53f04f4be88d939c13ed2a4be9_1763086959702__UDF43-m4_17ck_; kampyleUserSession=1763087170550; kampyleUserSessionsCount=19; kampyleUserPercentile=8.79202537826622; s_sq=%5B%5BB%5D%5D; _pxhd=uak0IorFpv1fnKcDbTolpVVyYoQgveh8Vu5npt-jpozg2jmS74trJ5PICFu4pnEDbtz6G5y5v2WUWGDfiLGE0w==:yA8IbFK9yGYc-k4OpU3mEvN4mlEegJA6lOhUQSaKY182xywWyzNZjblPmna5baVoF3sM2b9MAUrwMvBAaCKNGdVlkzucOfg/XmUTCjqKras=; mt.sc=%7B%22i%22%3A1763093198231%2C%22d%22%3A%5B%5D%7D; _cs_mk_aa=0.22273388122305382_1763093203783; kndctr_F18502BE5329FB670A490D4C_AdobeOrg_cluster=va6; _px2=eyJ1IjoiODkwYTBiMmEtYzExMi0xMWYwLThhMjctYjNkOWRmNWNhOGQ1IiwidiI6ImVhOTk2NTA4LWJkOWItMTFmMC04OGIyLWJhZmZlY2M3NWJiZSIsInQiOjE1NjE1MDcyMDAwMDAsImgiOiJhZjExNDRlZjk3Y2UyNzUyMjQzYzU2YmI0ZGY5NWNlMWU0ZGU0MjJiZTk2OTA1ODcyMmYyNmEwYzViMTEzN2Q0In0=; OptanonConsent=isGpcEnabled=0&datestamp=Fri+Nov+14+2025+09%3A30%3A31+GMT%2B0500+(Pakistan+Standard+Time)&version=202506.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&landingPath=NotLandingPage&groups=1%3A1%2C3%3A1%2CBG71%3A1%2C2%3A1%2C4%3A1&AwaitingReconsent=false; pageNameDuplicate=en_us:pdp:wool%20cashmereturtlenecksweater; s_ips=2075; BVBRANDSID=bd3e2d25-1eee-41a0-ae5d-fec2a7f3154b; __cq_bc=%7B%22bbxf-RalphLauren_US%22%3A%5B%7B%22id%22%3A%22100068384%22%7D%2C%7B%22id%22%3A%22636761%22%7D%2C%7B%22id%22%3A%22100007783%22%7D%2C%7B%22id%22%3A%22100056752%22%7D%2C%7B%22id%22%3A%22100058406%22%7D%2C%7B%22id%22%3A%22100034094%22%7D%2C%7B%22id%22%3A%22100055180%22%7D%5D%7D; __cq_seg=0~0.42!1~0.03!2~0.58!3~0.28!4~-0.23!5~0.33!6~-0.05!7~-0.48!8~-0.12!9~-0.06!f0~15~5; s_tp=7755; s_ppv=en_us%253Apdp%253Awool%2520cashmereturtlenecksweater%2C27%2C27%2C2075%2C3%2C11; _br_uid_2=uid%3D5781677485164%3Av%3D17.0%3Ats%3D1763012829120%3Ahc%3D51%3Acdp_segments%3DNjczNGUyNGQxOWVkN2NjNGQ3NmY3ZDgzOjY3MzYwNTRkNTljMDBiZjgxNjY3YzYyMw%3D%3D; RT="z=1&dm=ralphlauren.com&si=d8f32cac-9583-47f0-8fa2-28fcdc1f20b0&ss=mhyc5caf&sl=3&tt=u7t&bcn=%2F%2F173bf110.akstat.io%2F&ld=vq42"; _cs_id=ff766c0c-3ae7-abe0-b056-c6dd125dccee.1763012821.6.1763094645.1763093148.1751437089.1797176821615.1.x; _cs_s=4.0.U.9.1763096445420; _scid_r=NSK4X_ffRgRqa85UfM3NsOm3dNF_zBP0N7XaJQ; forterToken=59625d53f04f4be88d939c13ed2a4be9_1763094631168__UDF43-mnf-a4_17ck_; ttcsid=1763093218300::ZtQLsai5zfUZ9EEcZMGM.8.1763094648689.0; ttcsid_C07EMMSBPACVH56APG1G=1763093218298::s3VLVl-YBUpFqsqcwnDU.8.1763094648689.0; __rtbh.lid=%7B%22eventType%22%3A%22lid%22%2C%22id%22%3A%227jNdMj5JWC6WbNAt7SPG%22%2C%22expiryDate%22%3A%222026-11-14T04%3A30%3A50.867Z%22%7D; _ga_MHJQ8DE280=GS2.1.s1763093217$o8$g1$t1763094653$j54$l0$h0; _derived_epik=dj0yJnU9VXpTeTg0bnJLb1g2Z09zYUVzaFdxMDhobG1KeE9kdGMmbj1WLXVuZUJCZmd0RkpobnhNRE5USFBRJm09MSZ0PUFBQUFBR2tXc0lJJnJtPTEmcnQ9QUFBQUFHa1dzSUkmc3A9NQ; _ga=GA1.2.1675656094.1763034144; _gat_gtag_UA_106096199_1=1; _bti=%7B%22app_id%22%3A%22rl-na%22%2C%22bsin%22%3A%2223y5JwtWQ%2F9Rh7PMiFIzfcfj10KdFEtEK88eaxNdpsePLyS54M9R9rJCtiMikOtEN6zYnCkCw0qRs%2BrNXqLG%2FQ%3D%3D%22%2C%22is_identified%22%3Afalse%7D; kampyleSessionPageCounter=14; __rtbh.uid=%7B%22eventType%22%3A%22uid%22%2C%22id%22%3A%22%7B%7BUID%7D%7D%22%2C%22expiryDate%22%3A%222026-11-14T04%3A31%3A01.984Z%22%7D; _uetsid=34b28250c05411f083507bd00bd384ae; _uetvid=34b2f250c05411f0af9f490f9145ba38; mp_ralph_lauren_mixpanel=%7B%22distinct_id%22%3A%20%2219a7bc102889-0f7a08a5db425d-26061b51-100200-19a7bc10289587%22%2C%22bc_persist_updated%22%3A%201763012838029%7D; bc_invalidateUrlCache_targeting=1763094663856',
      referer: "https://www.ralphlauren.com/",
      priority: "u=0, i",
      "sec-ch-ua":
        '"Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"',
      "sec-ch-ua-mobile": "?0",
      "sec-ch-ua-platform": '"Windows"',
      "sec-fetch-dest": "document",
      "sec-fetch-mode": "navigate",
      "sec-fetch-site": "none",
      "sec-fetch-user": "?1",
      "upgrade-insecure-requests": "1",
      "user-agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
    };

    // Construct AJAX URL for specific color
    const encodedColor = encodeURIComponent(colorName);
    const ajaxUrl = `https://www.ralphlauren.com/${productId}.html?format=ajax&source=rlQuickshop&pageType=search&dwvar_${productId}_colorname=${encodedColor}&cgid=women-clothing`;

    console.log(`      Fetching color: ${colorName}`);

    const response = await retryRequestWithProxyRotation(
      async (axiosInstance) => {
        return await axiosInstance.get(ajaxUrl, {
          headers,
          maxRedirects: 5,
          validateStatus: function (status) {
            return status >= 200 && status < 400; // Accept redirects
          },
        });
      },
      5, // Increased retries
      3000, // Increased base delay
      "US"
    );

    if (!response || !response.data) {
      throw new Error("Empty response from server");
    }

    const $ = cheerio.load(response.data);

    // Extract sizes and availability for this specific color
    const sizes = [];
    $(
      ".size-swatches .variations-attribute, .js-attributes-list.size-swatches .variations-attribute"
    ).each((i, el) => {
      const size = $(el).find("span.attribute-value bdi").text().trim();
      if (size) {
        const availability = $(el).hasClass("out")
          ? "Unavailable"
          : "Available";
        sizes.push({ size, availability });
      }
    });

    // Extract images for this specific color
    const images = [];
    $(".swiper-zoom-container picture").each((i, el) => {
      const img = $(el).find("img").attr("data-img");
      if (img) images.push(img);
    });

    return {
      sizes: sizes.map((s) => ({
        size: s.size,
        isInStock: s.availability === "Available",
      })),
      images: images, // Add images for this specific color
    };
  } catch (error) {
    console.error(`      Error fetching color ${colorName}: ${error.message}`);
    if (error.response) {
      console.error(`      Status: ${error.response.status}`);
    }
    return null;
  }
}

// Function to extract product details using axios and cheerio (EXACT SAME AS ralphlaurenDetail.js)
async function extractProductDetails(productUrl, cookieString = "") {
  try {
    const headers = {
      accept:
        "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
      "accept-encoding": "gzip, deflate, br, zstd",
      "accept-language": "en-AU,en-GB;q=0.9,en-US;q=0.8,en;q=0.7",
      "cache-control": "max-age=0",
      cookie:
        'dwanonymous_55b6a3b329e729876c1d594e39f4ac4e=acaTqe7Av6CG5NyBk6ttYFK2cd; pzcookie="{"pz_id":"","EP_RID":""}"; _pxvid=ea996508-bd9b-11f0-88b2-baffecc75bbe; __pxvid=facd4d8e-c053-11f0-abb9-86ca80cf842b; _fbp=fb.1.1763012739255.336156945516096323; crl8.fpcuid=8c310380-6654-491b-9067-f3737cca984c; s_fid=14E4A1A092E1A9B1-0C9A602E97D9FD7F; _scid=E6K4X_ffRgRqa85UfM3NsOm3dNF_zBP0; _ScCbts=%5B%22574%3Bchrome.2%3A2%3A5%22%5D; _tt_enable_cookie=1; _ttp=01K9XVZ80GRHA6A2P4CH8X5V46_.tt.1; s_vi=[CS]v1|348AB846DD33C8A5-400002DC2134BABB[CE]; _sctr=1%7C1762974000000; mt.v=5.1698363828.1763012458976; headerDefault=1; _gid=GA1.2.485205492.1763012787; _gcl_au=1.1.1531361348.1763012792; __exponea_etc__=f26230b1-83c5-4a5b-b969-aecca0bfa66d; BVBRANDID=4315119d-cec0-43db-842b-9db4cac24baa; _pin_unauth=dWlkPVl6Y3hOV00xTm1VdFpEQm1aQzAwT1RobUxXSTFZbUV0WVdVNVl6QmtaamhqTUdJMw; _cs_c=0; rmStore=atm:pixel; kampyle_userid=6a12-4321-8c44-556e-3087-6698-c54a-1d27; _lc2_fpi=dafbb3cf7621--01k9xw1wh2xxhet6cm2xpsstd9; afUserId=5f9bd9e6-0776-449d-8099-ba2a40c65b6a-p; AF_SYNC=1763012836430; _li_ss=CgA; kndctr_F18502BE5329FB670A490D4C_AdobeOrg_identity=CiYwNzg4ODcxMjcwODIwMDMwNTQzMjU3MjQ1MDM3MjIxMzc5MjY0NFIQCMTDg96nMxgBKgNWQTYwAfABxMOD3qcz; bluecoreNV=false; AMCV_F18502BE5329FB670A490D4C%40AdobeOrg=179643557%7CMCMID%7C07888712708200305432572450372213792644%7CMCAID%7C348AB846DD33C8A5-400002DC2134BABB%7CvVersion%7C5.5.0; dwac_102c95db27e6f188d36d6303ba=PPbDY-KG7GBcDH8xjF4AuP3VzzHGwswSsB4%3D|dw-only|||USD|false|US%2FEastern|true; cqcid=acaTqe7Av6CG5NyBk6ttYFK2cd; cquid=||; sid=PPbDY-KG7GBcDH8xjF4AuP3VzzHGwswSsB4; dwsid=Yw00b6W99SSD5vma-KN9rqIuOD2B-dVWb_R8DJXqwgr21taci191JKzfpiaAQ9En7eSLyJUO1o9y5hDXyzv71g==; _pxhd=BWVJKaI9I0lvQ9QnJBiLgwPCVtOvB8keJ54vwLUOJCImpbUDcnXrINLRlkutWw40GvpKurB9LzkrRtCT-0g16Q==:8bOePWa6VYmsdaJzNXPv9W3aMdkmthd9GDGAJksoeethT1xOqQfCHIlCGaDkiHcBIIIkUJrMnr9zKb6-8ZAOXFhqG80OXNwBFPmRToD0Byc=; __cq_dnt=0; dw_dnt=0; s_cc=true; dw=1; dw_cookies_accepted=1; __cq_uuid=acRCt9DzhyGBPq83a8y7ubEXF6; pxcts=4d8d9803-c089-11f0-8fde-e23b1699e551; _cs_cvars=%7B%7D; _li_dcdm_c=.ralphlauren.com; _lc2_fpi_js=dafbb3cf7621--01k9xw1wh2xxhet6cm2xpsstd9; gender_browse_affinity=women; __cq_bc=%7B%22bbxf-RalphLauren_US%22%3A%5B%7B%22id%22%3A%22100058406%22%7D%5D%7D; __cq_seg=0~0.06!1~0.00!2~-0.26!3~-0.77!4~-0.18!5~-0.29!6~-0.10!7~-0.13!8~0.29!9~0.32!f0~15~5; mt.division=women; mt.brand=polo ralph lauren; BVImplfy24_redesign=13693_12_0; pageNameDuplicate=en_us:pdp:linen%20blendherringbonemockneckjacket; OptanonConsent=isGpcEnabled=0&datestamp=Thu+Nov+13+2025+17%3A10%3A39+GMT%2B0500+(Pakistan+Standard+Time)&version=202506.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&landingPath=NotLandingPage&groups=1%3A1%2C3%3A1%2CBG71%3A1%2C2%3A1%2C4%3A1&AwaitingReconsent=false; _scid_r=NCK4X_ffRgRqa85UfM3NsOm3dNF_zBP0N7Xa9w; __rtbh.lid=%7B%22eventType%22%3A%22lid%22%2C%22id%22%3A%227jNdMj5JWC6WbNAt7SPG%22%2C%22expiryDate%22%3A%222026-11-13T12%3A10%3A46.898Z%22%7D; _ga=GA1.2.1675656094.1763034144; _derived_epik=dj0yJnU9TXd5aDBkRXNXQ3J0X2drQnVCUEcwVDNKbWc5RC03MC0mbj1McjRyQnpZODdUdk1ud0lNRHF0N1JnJm09MSZ0PUFBQUFBR2tWeXN3JnJtPTEmcnQ9QUFBQUFHa1Z5c3cmc3A9NQ; forterToken=59625d53f04f4be88d939c13ed2a4be9_1763035838304__UDF43-m4_17ck_; _ga_MHJQ8DE280=GS2.1.s1763035633$o2$g1$t1763035854$j17$l0$h0; _br_uid_2=uid%3D5781677485164%3Av%3D17.0%3Ats%3D1763012829120%3Ahc%3D7; __rtbh.uid=%7B%22eventType%22%3A%22uid%22%2C%22id%22%3A%22%7B%7BUID%7D%7D%22%2C%22expiryDate%22%3A%222026-11-13T12%3A11%3A00.400Z%22%7D; kampyleUserSession=1763035860557; kampyleUserSessionsCount=4; kampyleUserPercentile=97.93816228447476; kampyleSessionPageCounter=1; mp_ralph_lauren_mixpanel=%7B%22distinct_id%22%3A%20%2219a7bc102889-0f7a08a5db425d-26061b51-100200-19a7bc10289587%22%2C%22bc_persist_updated%22%3A%201763012838029%7D; _uetsid=34b28250c05411f083507bd00bd384ae; _uetvid=34b2f250c05411f0af9f490f9145ba38; _bti=%7B%22app_id%22%3A%22rl-na%22%2C%22bsin%22%3A%222kJdziotmQ%2BBZkMVXXg6zmAxKj%2FCj5HGTQ96gmqJ7KBCFCyEpz8yk63OxnOG6cy6oKRYSN6BwYitaiNALQFzDA%3D%3D%22%2C%22is_identified%22%3Afalse%7D; ttcsid=1763035635338::rPTXmg_1WcY420vTkF-g.2.1763035952847.0; ttcsid_C07EMMSBPACVH56APG1G=1763035635336::cCyKMTcUfX23AV4dCB6H.2.1763035952847.0; s_sq=%5B%5BB%5D%5D; s_ips=680; s_tp=7573; s_ppv=en_us%253Apdp%253Alinen%2520blendherringbonemockneckjacket%2C34%2C9%2C2547%2C3%2C11; _px2=eyJ1IjoiNjA4NTBiZjAtYzA4OS0xMWYwLWE3MDEtYmJjYTAyZTY5MzVlIiwidiI6ImVhOTk2NTA4LWJkOWItMTFmMC04OGIyLWJhZmZlY2M3NWJiZSIsInQiOjE1Mjk5NzEyMDAwMDAsImgiOiIwNTBhODQ4ZjFkZDM3OTgxMDQ5NDgwMDZiN2Q3ODRiMTljOWY2OWU5OWUyMzMyNTI0NjdkNTExM2Q4NjY2MzdhIn0=; RT="z=1&dm=ralphlauren.com&si=d8f32cac-9583-47f0-8fa2-28fcdc1f20b0&ss=mhxe0jqu&sl=0&tt=0&bcn=%2F%2F684d0d47.akstat.io%2F"; _cs_id=ff766c0c-3ae7-abe0-b056-c6dd125dccee.1763012821.3.1763040831.1763040831.1751437089.1797176821615.1.x; _cs_s=1.0.U.9.1763042632076',
      referer: "https://www.ralphlauren.com/",
      priority: "u=0, i",
      "sec-ch-ua":
        '"Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"',
      "sec-ch-ua-mobile": "?0",
      "sec-ch-ua-platform": '"Windows"',
      "sec-fetch-dest": "document",
      "sec-fetch-mode": "navigate",
      "sec-fetch-site": "none",
      "sec-fetch-user": "?1",
      "upgrade-insecure-requests": "1",
      "user-agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
    };

    console.log(`    Fetching: ${productUrl}`);

    const response = await retryRequestWithProxyRotation(
      async (axiosInstance) => {
        return await axiosInstance.get(productUrl, {
          headers,
          maxRedirects: 5,
          validateStatus: function (status) {
            return status >= 200 && status < 400; // Accept redirects
          },
        });
      },
      5, // Increased retries
      3000, // Increased base delay
      "US"
    );

    if (!response || !response.data) {
      throw new Error("Empty response from server");
    }

    const $ = cheerio.load(response.data);

    // Extract alternative images
    const images = [];
    $(".swiper-zoom-container picture").each((i, el) => {
      const img = $(el).find("img").attr("data-img");
      if (img) images.push(img);
    });

    // Extract colors
    const colors = [];
    $(".color-swatches .variations-attribute").each((i, el) => {
      const link = $(el).find("a.js-variation-link");
      const img = $(el).find("img");
      if (link.length) {
        const colorName = link.attr("data-color") || "";
        if (colorName) {
          const isSelected = $(el).hasClass("selected");
          const swatchImg = img.length ? img.attr("src") : "";
          colors.push({
            color: colorName,
            isSelected: isSelected,
            swatchImage: swatchImg,
          });
        }
      }
    });

    // Extract sizes and availability (only from size-swatches)
    const sizes = [];
    $(
      ".size-swatches .variations-attribute, .js-attributes-list.size-swatches .variations-attribute"
    ).each((i, el) => {
      const size = $(el).find("span.attribute-value bdi").text().trim();
      // Skip empty sizes
      if (size) {
        const availability = $(el).hasClass("out")
          ? "Unavailable"
          : "Available";
        sizes.push({ size, availability });
      }
    });

    // Extract product description and material
    const description = [];
    $(".js-product-details .bullet-list ul li").each((i, el) => {
      description.push($(el).text().trim());
    });

    // Extract review count
    let reviewCount = $(".bvseo-reviewCount").text().trim();
    reviewCount = reviewCount ? parseInt(reviewCount) : 0;

    // Extract average rating
    let averageRating = $(".bvseo-ratingValue").text().trim();
    averageRating = averageRating ? parseFloat(averageRating) : 0;

    // Extract ratings count (if same as review count, can reuse)
    let ratingsCount = reviewCount;

    // Extract style number
    const styleNumber = $(
      ".js-product-details .style-number span.screen-reader-digits"
    )
      .text()
      .trim();

    // Extract price
    let price = 0;
    const priceText = $(".lowblack").first().text().trim();
    if (priceText) {
      const priceMatch = priceText.match(/[\d,]+/);
      if (priceMatch) {
        price = parseFloat(priceMatch[0].replace(/,/g, ""));
      }
    }

    // Extract currency
    const currency = priceText.includes("Rs") ? "PKR" : "USD";

    // Extract category from breadcrumbs
    let category = "";
    const breadcrumbElement = $("[data-monetatebreadcrumbs]").first();
    if (breadcrumbElement.length) {
      try {
        const breadcrumbs = JSON.parse(
          breadcrumbElement.attr("data-monetatebreadcrumbs")
        );
        category = breadcrumbs.slice(1).join(" > ");
      } catch (e) {
        category = "";
      }
    }

    // Check if we got essential data
    if (colors.length === 0) {
      console.error(`    No colors found for product`);
      return null;
    }

    return {
      images,
      colors,
      sizes: sizes.map((s) => ({
        size: s.size,
        isInStock: s.availability === "Available",
      })),
      description: description.join(" "),
      review_count: reviewCount,
      average_ratings: averageRating,
      ratings_count: ratingsCount,
      styleNumber,
      price,
      currency,
      category,
    };
  } catch (error) {
    console.error(`    Error fetching product details: ${error.message}`);
    if (error.response) {
      console.error(`    Status: ${error.response.status}, URL: ${productUrl}`);
    }
    return null;
  }
}

async function scrapeRalphLauren() {
  let browser;
  try {
    console.log("Launching browser...");
    browser = await puppeteer.launch({
      headless: false, // Set to true in production
      args: [
        "--no-sandbox",
        "--disable-setuid-sandbox",
        "--disable-blink-features=AutomationControlled",
        "--disable-web-security",
        "--disable-features=IsolateOrigins,site-per-process",
      ],
    });

    const page = await browser.newPage();

    // Set viewport
    await page.setViewport({ width: 1920, height: 1080 });

    // Set extra headers
    await page.setExtraHTTPHeaders({
      "Accept-Language": "en-US,en;q=0.9",
      Accept:
        "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    });

    const allProducts = [];
    const productIdSet = new Set(); // To avoid duplicates
    const genderCounts = { Male: 0, Female: 0 };
    const TARGET_PRODUCTS_PER_GENDER = 1000;

    // Helper function to scroll and load more products
    async function scrollToLoadMore(page, maxScrolls = 100) {
      let previousProductCount = 0;
      let scrollAttempts = 0;
      let noNewProductsCount = 0;

      while (scrollAttempts < maxScrolls) {
        // Get current product count
        const currentProductCount = await page.evaluate(() => {
          return document.querySelectorAll(".product-tile").length;
        });

        // If no new products loaded after 3 attempts, stop
        if (currentProductCount === previousProductCount) {
          noNewProductsCount++;
          if (noNewProductsCount >= 3) {
            console.log(
              `No new products loaded after ${noNewProductsCount} scroll attempts. Stopping.`
            );
            break;
          }
        } else {
          noNewProductsCount = 0; // Reset counter if new products found
          console.log(
            `Scroll attempt ${
              scrollAttempts + 1
            }: Loaded ${currentProductCount} products (${
              currentProductCount - previousProductCount
            } new)`
          );
        }

        previousProductCount = currentProductCount;

        // Scroll to bottom gradually
        await page.evaluate(() => {
          window.scrollBy(0, window.innerHeight);
        });

        // Small delay before scrolling to bottom
        await new Promise((resolve) => setTimeout(resolve, 500));

        // Scroll to absolute bottom
        await page.evaluate(() => {
          window.scrollTo(0, document.body.scrollHeight);
        });

        // Wait for potential new content to load (increased wait time for lazy loading)
        await new Promise((resolve) => setTimeout(resolve, 4000));

        // Double-check product count after waiting
        const recheckCount = await page.evaluate(() => {
          return document.querySelectorAll(".product-tile").length;
        });

        // If we got new products, wait a bit more for images/content to fully load
        if (recheckCount > currentProductCount) {
          await new Promise((resolve) => setTimeout(resolve, 2000));
        }

        // Check for "Load More" button and click it if exists
        try {
          const loadMoreButton = await page.evaluateHandle(() => {
            // Try multiple selectors for load more buttons
            const selectors = [
              'button[class*="load-more"]',
              'a[class*="load-more"]',
              ".show-more",
              'button[class*="show-more"]',
              'a[class*="show-more"]',
            ];

            for (const selector of selectors) {
              const btn = document.querySelector(selector);
              if (btn) {
                const text = btn.textContent || btn.innerText || "";
                if (
                  text.toLowerCase().includes("load more") ||
                  text.toLowerCase().includes("show more") ||
                  text.toLowerCase().includes("view more")
                ) {
                  return btn;
                }
              }
            }

            // Also check for buttons with specific text
            const buttons = Array.from(document.querySelectorAll("button, a"));
            for (const btn of buttons) {
              const text = (
                btn.textContent ||
                btn.innerText ||
                ""
              ).toLowerCase();
              if (
                text.includes("load more") ||
                text.includes("show more") ||
                text.includes("view more")
              ) {
                return btn;
              }
            }

            return null;
          });

          if (loadMoreButton && loadMoreButton.asElement()) {
            const isVisible = await page.evaluate((btn) => {
              const rect = btn.getBoundingClientRect();
              return (
                rect.top >= 0 &&
                rect.left >= 0 &&
                rect.bottom <=
                  (window.innerHeight ||
                    document.documentElement.clientHeight) &&
                rect.right <=
                  (window.innerWidth || document.documentElement.clientWidth)
              );
            }, loadMoreButton);

            if (isVisible) {
              console.log("Clicking 'Load More' button...");
              await loadMoreButton.asElement().click();
              await new Promise((resolve) => setTimeout(resolve, 3000));
            }
          }
        } catch (error) {
          // No load more button, continue scrolling
        }

        scrollAttempts++;
      }

      return previousProductCount;
    }

    // Loop through each category
    for (let catIndex = 0; catIndex < CATEGORIES.length; catIndex++) {
      const categoryInfo = CATEGORIES[catIndex];

      // Skip if we already have enough products for this gender
      if (genderCounts[categoryInfo.gender] >= TARGET_PRODUCTS_PER_GENDER) {
        console.log(
          `\n=== Skipping Category ${catIndex + 1}/${CATEGORIES.length}: ${
            categoryInfo.category
          } (${categoryInfo.gender}) ===`
        );
        console.log(
          `Already have ${genderCounts[categoryInfo.gender]} products for ${
            categoryInfo.gender
          } (target: ${TARGET_PRODUCTS_PER_GENDER})`
        );
        continue;
      }

      // Wrap category scraping in try-catch to continue on errors
      try {
        console.log(
          `\n=== Scraping Category ${catIndex + 1}/${CATEGORIES.length}: ${
            categoryInfo.category
          } (${categoryInfo.gender}) ===`
        );
        console.log(
          `Current count for ${categoryInfo.gender}: ${
            genderCounts[categoryInfo.gender]
          }/${TARGET_PRODUCTS_PER_GENDER}`
        );

        console.log(`Navigating to ${categoryInfo.url}...`);

        try {
          await page.goto(categoryInfo.url, {
            waitUntil: "domcontentloaded",
            timeout: 60000,
          });
        } catch (error) {
          console.error(`    ⚠ Navigation error: ${error.message}`);
          console.log(`    Skipping this category and continuing...`);
          continue;
        }

        // Wait for products to load with multiple selector attempts
        console.log("Waiting for products to load...");
        let productsFound = false;
        const selectors = [
          ".product-tile",
          ".product",
          "[data-masterid]",
          ".tile-product",
        ];

        for (const selector of selectors) {
          try {
            await page.waitForSelector(selector, { timeout: 15000 });
            productsFound = true;
            console.log(`    Found products using selector: ${selector}`);
            break;
          } catch (error) {
            // Try next selector
            continue;
          }
        }

        if (!productsFound) {
          console.error(
            `    ⚠ No products found with any selector. Skipping category...`
          );
          continue;
        }

        // Give extra time for any dynamic content to fully render
        console.log("Products found, waiting for dynamic content...");
        await new Promise((resolve) => setTimeout(resolve, 2000));

        // Try to click "View All" button if it exists
        try {
          const viewAllButton = await page.$("a.view-all");
          if (viewAllButton) {
            console.log("Clicking 'View All' button...");
            await viewAllButton.click();
            await new Promise((resolve) => setTimeout(resolve, 3000));
            console.log("View All clicked, now loading all products...");
          } else {
            console.log(
              "No 'View All' button found, proceeding with visible products"
            );
          }
        } catch (error) {
          console.log(
            "Could not click 'View All' button, proceeding with visible products"
          );
        }

        // Scroll to load all products via infinite scroll
        console.log("Scrolling to load all products...");
        const totalProductsLoaded = await scrollToLoadMore(page, 100);
        console.log(
          `Finished scrolling. Total products loaded: ${totalProductsLoaded}`
        );

        // Extract product data from this category
        const products = await page.evaluate((catGender) => {
          // Try multiple selectors to find products
          let productElements = document.querySelectorAll(".product-tile");
          if (productElements.length === 0) {
            productElements = document.querySelectorAll(".product");
          }
          if (productElements.length === 0) {
            productElements = document.querySelectorAll("[data-masterid]");
          }
          if (productElements.length === 0) {
            productElements = document.querySelectorAll(".tile-product");
          }
          const productsData = [];

          productElements.forEach((productTile) => {
            try {
              // Extract basic product info from data attributes
              const parentProductId =
                productTile.getAttribute("data-masterid") || "";
              const name = productTile.getAttribute("data-pname") || "";

              // Skip if no product ID or name
              if (!parentProductId || !name) {
                return;
              }

              // Extract brand name
              const brandElement = productTile.querySelector(".brand-name");
              const brand = brandElement
                ? brandElement.textContent.trim()
                : "Polo Ralph Lauren";

              // Extract product link
              const linkElement = productTile.querySelector(".name-link");
              const productUrl = linkElement
                ? linkElement.getAttribute("href")
                : "";
              const fullUrl = productUrl
                ? `https://www.ralphlauren.com${productUrl}`
                : "";

              // Extract main image (from active swiper slide or first image)
              let imageUrl = "";
              const activeSlideImg = productTile.querySelector(
                ".swiper-slide-active img"
              );
              const firstImg = productTile.querySelector(".first-image img");
              const anyImg = productTile.querySelector("img");

              if (activeSlideImg) {
                imageUrl =
                  activeSlideImg.getAttribute("src") ||
                  activeSlideImg.getAttribute("srcset") ||
                  "";
              } else if (firstImg) {
                imageUrl =
                  firstImg.getAttribute("src") ||
                  firstImg.getAttribute("srcset") ||
                  "";
              } else if (anyImg) {
                imageUrl =
                  anyImg.getAttribute("src") ||
                  anyImg.getAttribute("srcset") ||
                  "";
              }

              // Extract all available color swatches and create variant objects
              const swatchElements =
                productTile.querySelectorAll(".swatch-item");
              const variants = [];

              swatchElements.forEach((swatch) => {
                const colorLink = swatch.querySelector("a.swatch");
                if (colorLink) {
                  const colorName = colorLink.getAttribute("data-color") || "";
                  const variantId =
                    colorLink.getAttribute("data-variantid") || "";
                  const swatchImage = swatch.querySelector(".swatch-image");
                  const swatchImgUrl = swatchImage
                    ? swatchImage.getAttribute("src")
                    : "";

                  // Get color-specific product URL
                  const colorUrl = colorLink.getAttribute("href") || "";
                  const colorProductUrl = colorUrl
                    ? `https://www.ralphlauren.com${colorUrl}`
                    : fullUrl;

                  if (colorName) {
                    const variant = {
                      price_currency: "USD",
                      original_price: 0,
                      link_url: colorProductUrl,
                      deeplink_url: colorProductUrl,
                      image_url: swatchImgUrl || "",
                      alternate_image_urls: [],
                      is_on_sale: false,
                      is_in_stock: true,
                      size: "",
                      size_label: "",
                      color: colorName,
                      mpn: "",
                      ratings_count: 0,
                      average_ratings: 0,
                      review_count: 0,
                      selling_price: 0,
                      sale_price: 0,
                      final_price: 0,
                      discount: 0,
                      operation_type: "INSERT",
                      variant_id: variantId || "",
                      variant_description: "",
                    };

                    variants.push(variant);
                  }
                }
              });

              // Create product object
              const productData = {
                parent_product_id: parentProductId,
                name: name,
                description: "",
                category: "",
                brand: brand,
                retailer_domain: "ralphlauren.global",
                gender: catGender,
                materials: "",
                return_policy_link: "",
                return_policy: "",
                size_chart: "",
                available_bank_offers: "",
                available_coupons: "",
                variants: variants,
                operation_type: "INSERT",
                source: "ralphlauren",
                product_url: fullUrl,
              };

              productsData.push(productData);
            } catch (error) {
              console.error("Error parsing product tile:", error);
            }
          });

          return productsData;
        }, categoryInfo.gender); // Pass gender to page.evaluate

        console.log(`Found ${products.length} products in this category`);

        // Add products to allProducts, avoiding duplicates and tracking by gender
        let newProductsAdded = 0;
        products.forEach((product) => {
          if (
            !productIdSet.has(product.parent_product_id) &&
            genderCounts[categoryInfo.gender] < TARGET_PRODUCTS_PER_GENDER
          ) {
            productIdSet.add(product.parent_product_id);
            allProducts.push(product);
            genderCounts[categoryInfo.gender]++;
            newProductsAdded++;
          }
        });

        console.log(
          `Added ${newProductsAdded} new products. Total unique products: ${allProducts.length}`
        );
        console.log(
          `Gender counts - Male: ${genderCounts.Male}, Female: ${genderCounts.Female}`
        );

        // Small delay between categories
        await new Promise((resolve) => setTimeout(resolve, 1000));
      } catch (error) {
        console.error(
          `\n⚠ Error scraping category ${categoryInfo.category} (${categoryInfo.gender}): ${error.message}`
        );
        console.log(`Continuing with next category...\n`);
        // Small delay before continuing
        await new Promise((resolve) => setTimeout(resolve, 1000));
        continue;
      }
    }

    console.log(`\n=== Finished scraping all categories ===`);
    console.log(`Total unique products collected: ${allProducts.length}`);
    console.log(`Male products: ${genderCounts.Male}`);
    console.log(`Female products: ${genderCounts.Female}`);

    // Close browser after getting product list
    console.log("Closing browser...");
    await browser.close();
    browser = null;

    // Setup output directory
    const outputDir = path.join(
      __dirname,
      "output",
      STORE_DETAILS.country,
      `ralphlauren-${STORE_DETAILS.country}`
    );
    if (!fs.existsSync(outputDir)) {
      fs.mkdirSync(outputDir, { recursive: true });
    }

    const catalogPath = path.join(outputDir, "catalog.json");

    // Fill in store details for all products
    allProducts.forEach((product) => {
      product.return_policy_link = STORE_DETAILS.return_policy_link;
      product.return_policy = STORE_DETAILS.return_policy;
      product.size_chart = STORE_DETAILS.size_chart;
      // Remove internal fields before saving
      delete product.product_url;
    });

    // Save product list immediately after scraping
    const catalog = {
      store_info: {
        name: "Ralph Lauren",
        domain: "ralphlauren.global",
        currency: STORE_DETAILS.currency,
        country: STORE_DETAILS.country,
        total_products: allProducts.length,
        total_variants: allProducts.reduce(
          (sum, p) => sum + (p.variants ? p.variants.length : 0),
          0
        ),
        categories: [
          ...new Set(allProducts.map((p) => p.category).filter((c) => c)),
        ],
        crawled_at: new Date().toISOString(),
      },
      products: allProducts,
    };

    // Save to file
    fs.writeFileSync(catalogPath, JSON.stringify(catalog, null, 2));
    console.log(
      `\n✅ Saved ${allProducts.length} products (${catalog.store_info.total_variants} variants) to ${catalogPath}`
    );

    console.log("\n=== Scraping Summary ===");
    console.log(`Total products: ${allProducts.length}`);
    console.log(`Total variants: ${catalog.store_info.total_variants}`);
    console.log(`Male products: ${genderCounts.Male}`);
    console.log(`Female products: ${genderCounts.Female}`);
    if (allProducts.length > 0) {
      console.log("\nSample product:");
      console.log(JSON.stringify(allProducts[0], null, 2));
    }

    // COMMENTED OUT: Product details fetching section
    /*
    // Get cookies from the browser session BEFORE closing
    const cookies = await page.cookies();
    const cookieString = cookies.map((c) => `${c.name}=${c.value}`).join("; ");
    console.log(`\nExtracted ${cookies.length} cookies from browser session`);

    // Function to save products incrementally
    async function saveProductsIncremental(enrichedProducts) {
      const catalog = {
        store_info: {
          name: "Ralph Lauren",
          domain: "ralphlauren.global",
          currency: STORE_DETAILS.currency,
          country: STORE_DETAILS.country,
          total_products: enrichedProducts.length,
          total_variants: enrichedProducts.reduce(
            (sum, p) => sum + (p.variants ? p.variants.length : 0),
            0
          ),
          categories: [
            ...new Set(
              enrichedProducts.map((p) => p.category).filter((c) => c)
            ),
          ],
          crawled_at: new Date().toISOString(),
        },
        products: enrichedProducts,
      };

      fs.writeFileSync(catalogPath, JSON.stringify(catalog, null, 2));
      console.log(
        `\n💾 Saved ${enrichedProducts.length} products (${catalog.store_info.total_variants} variants) to ${catalogPath}`
      );
    }

    // Enrich products with detailed information using axios only
    console.log("\nFetching detailed product information using axios...");
    const enrichedProducts = []; // Only save new successfully processed products

    // Process all products (up to 1000 per gender)
    const productsToProcess = allProducts;
    console.log(`Processing ${productsToProcess.length} products\n`);

    let processedCount = 0;
    for (let i = 0; i < productsToProcess.length; i++) {
      const product = productsToProcess[i];
      console.log(
        `\nProcessing ${i + 1}/${productsToProcess.length}: ${product.name}`
      );

      // Use the main product URL from the listing page
      // Clean URL - remove query parameters that might cause issues
      let baseUrl = product.product_url;
      if (baseUrl && baseUrl.includes("?")) {
        baseUrl = baseUrl.split("?")[0];
      }

      // Fetch detailed product information using axios and cheerio with retry
      let details = null;
      let retries = 0;
      const maxRetries = 3;

      while (retries < maxRetries && !details) {
        try {
          details = await extractProductDetails(baseUrl, cookieString);
          if (details && details.colors && details.colors.length > 0) {
            break; // Success
          } else {
            // If details is null or has no colors, treat as failure and retry
            details = null;
            retries++;
            if (retries < maxRetries) {
              console.log(
                `    ⚠ No colors found, retry ${retries}/${maxRetries} for ${product.name}...`
              );
              await new Promise((resolve) =>
                setTimeout(resolve, 3000 * retries)
              );
            }
          }
        } catch (error) {
          retries++;
          if (retries < maxRetries) {
            console.log(
              `    ⚠ Error, retry ${retries}/${maxRetries} for ${product.name}: ${error.message}`
            );
            await new Promise((resolve) => setTimeout(resolve, 3000 * retries));
          } else {
            console.error(
              `    ✗ Failed after ${maxRetries} retries: ${error.message}`
            );
          }
        }
      }

      if (details && details.colors && details.colors.length > 0) {
        // Fill in product-level details
        product.description = details.description || "";
        product.category = details.category || "";
        product.return_policy_link = STORE_DETAILS.return_policy_link;
        product.return_policy = STORE_DETAILS.return_policy;
        product.size_chart = STORE_DETAILS.size_chart;
        // Preserve gender from category (already set when product was created)
        // Don't overwrite it - it comes from categoryInfo.gender

        const newVariants = [];

        // Fetch size details for each color separately
        for (const colorInfo of details.colors) {
          console.log(`    Processing color: ${colorInfo.color}`);

          // Fetch sizes for this specific color with retry
          let colorDetails = null;
          let colorRetries = 0;
          const maxColorRetries = 2;

          while (colorRetries < maxColorRetries && !colorDetails) {
            try {
              colorDetails = await extractColorDetails(
            product.parent_product_id,
            colorInfo.color,
            cookieString
          );
              if (
                colorDetails &&
                colorDetails.sizes &&
                colorDetails.sizes.length > 0
              ) {
                break; // Success
              } else {
                // If no sizes found, treat as failure and retry
                colorDetails = null;
                colorRetries++;
                if (colorRetries < maxColorRetries) {
                  await new Promise((resolve) =>
                    setTimeout(resolve, 2000 * colorRetries)
                  );
                }
              }
            } catch (error) {
              colorRetries++;
              if (colorRetries < maxColorRetries) {
                console.log(`      ⚠ Color fetch error, retrying...`);
                await new Promise((resolve) =>
                  setTimeout(resolve, 2000 * colorRetries)
                );
              }
            }
          }

          if (colorDetails && colorDetails.sizes.length > 0) {
            // Use images from colorDetails if available, otherwise fallback to details.images
            const colorImages =
              colorDetails.images && colorDetails.images.length > 0
                ? colorDetails.images
                : details.images;

            // Create variants for each size in this color
            colorDetails.sizes.forEach((sizeInfo) => {
              const variant = {
                price_currency: details.currency || STORE_DETAILS.currency,
                original_price: details.price,
                link_url: baseUrl.split("?")[0],
                deeplink_url: baseUrl.split("?")[0],
                image_url: colorImages[0] || "",
                alternate_image_urls: colorImages.slice(1) || [],
                is_on_sale: false,
                is_in_stock: sizeInfo.isInStock,
                size: sizeInfo.size,
                size_label: sizeInfo.size,
                color: colorInfo.color,
                mpn: uuidv5(
                  `${product.parent_product_id}-${colorInfo.color}`,
                  "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
                ),
                ratings_count: details.ratings_count || 0,
                average_ratings: details.average_ratings || 0,
                review_count: details.review_count || 0,
                selling_price: details.price,
                sale_price: 0,
                final_price: details.price,
                discount: 0,
                operation_type: "INSERT",
                variant_id:
                  `${product.parent_product_id}-${colorInfo.color}-${sizeInfo.size}`.replace(
                    /\s+/g,
                    "-"
                  ),
                variant_description: `${product.name} - ${colorInfo.color} - ${sizeInfo.size}`,
              };
              newVariants.push(variant);
            });
          }

          // Small delay between color requests to avoid rate limiting
          await new Promise((resolve) => setTimeout(resolve, 300));
        }

        product.variants = newVariants;
        console.log(`  ✓ Created ${newVariants.length} total variants`);
      } else {
        console.log(`  ⚠ Could not fetch details, keeping basic info`);
        // Fill in store details even if API call failed
        product.return_policy_link = STORE_DETAILS.return_policy_link;
        product.return_policy = STORE_DETAILS.return_policy;
        product.size_chart = STORE_DETAILS.size_chart;
      }

      // Remove internal fields before saving
      delete product.product_url;

      // Only add successfully processed products (with variants)
      if (product.variants && product.variants.length > 0) {
      enrichedProducts.push(product);
        processedCount++;

        // Save incrementally after every 10 products or at the end
        if (processedCount % 10 === 0 || i === productsToProcess.length - 1) {
          await saveProductsIncremental(enrichedProducts);
        }
      } else {
        console.log(`  ⚠ Skipping product (no variants): ${product.name}`);
      }

      // Add a delay between products to avoid rate limiting (reduced for speed)
      await new Promise((resolve) => setTimeout(resolve, 500));
    }

    // Final save with updated metadata
    const catalog = {
      store_info: {
        name: "Ralph Lauren",
        domain: "ralphlauren.global",
        currency: STORE_DETAILS.currency,
        country: STORE_DETAILS.country,
        total_products: enrichedProducts.length,
        total_variants: enrichedProducts.reduce(
          (sum, p) => sum + (p.variants ? p.variants.length : 0),
          0
        ),
        categories: [
          ...new Set(enrichedProducts.map((p) => p.category).filter((c) => c)),
        ],
        crawled_at: new Date().toISOString(),
      },
      products: enrichedProducts,
    };

    console.log("\n=== Scraping Summary ===");
    console.log(`Total products: ${enrichedProducts.length}`);
    console.log(`Total variants: ${catalog.store_info.total_variants}`);
    if (enrichedProducts.length > 0) {
    console.log("\nSample product:");
    console.log(JSON.stringify(enrichedProducts[0], null, 2));
    }

    // Final save
    fs.writeFileSync(catalogPath, JSON.stringify(catalog, null, 2));
    console.log(
      `\n✅ Final save: ${enrichedProducts.length} products (${catalog.store_info.total_variants} variants) saved to ${catalogPath}`
    );
    */

    return catalog;
  } catch (error) {
    console.error("Error during scraping:", error);
    throw error;
  } finally {
    if (browser) {
      await browser.close();
    }
  }
}

// Run the scraper
scrapeRalphLauren()
  .then(() => {
    console.log("Scraping completed successfully!");
    process.exit(0);
  })
  .catch((error) => {
    console.error("Scraping failed:", error);
    process.exit(1);
  });
