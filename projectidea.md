Plan for Automated Financial Statement Extraction and Analysis
Project Overview
We will build a pipeline to automate the extraction of key information from companies’ 10-K annual reports and compile it for analysis. The goal is to retrieve a company’s financial statements over the past 5–10 years, identify the most important data (both quantitative and qualitative), and store it in a structured format for further exploration and modeling. Eventually, this can be scaled to all publicly listed companies in an industry to compare performance trends across peers. For now, we’ll outline the process using a single example company (e.g. Apple Inc.) while keeping the approach general enough for any company or industry.
Key Steps:
Data Acquisition from SEC EDGAR: Find and download the company’s 10-K filings (annual reports) from the SEC’s EDGAR database.


Numeric Data Extraction: Parse the financial statements within each 10-K to pull out all relevant financial figures (revenue, expenses, profits, assets, etc.) for each year, using standardized (GAAP) definitions.


Textual Information Extraction: Identify important qualitative information in the 10-K (e.g. business description, management’s outlook, risk factors, organizational structure) and use AI to summarize or extract the key points.


Compilation into DataFrames: Assemble the extracted data into a structured format (e.g. a table or DataFrame). This will include a financial metrics DataFrame (primarily numeric, one row per year) and possibly a text insights DataFrame (summaries or key text snippets per year). The format will be optimized for analysis – machine-readable and consistent – rather than narrative readability.


Calculate Performance Metrics: From the raw financial data, compute additional business performance indicators (growth rates, margins, return ratios, etc.) for each year. These metrics help evaluate trends over time and facilitate comparison across companies.


Tooling – Hardcoded vs. AI: Determine which parts of the pipeline can be handled with deterministic code (hardcoded logic) and which require an AI agent for flexibility and comprehension. For example, scraping numeric tables can be code-driven, whereas interpreting management’s discussion likely needs an LLM (GPT/Gemini) for summarization. We will explicitly define every piece of information to extract and note whether extraction is via hardcoding or AI.


By carefully defining “key information” and structuring the extraction process, we ensure no important data is missed. Below is a detailed breakdown of each step.
1. Data Acquisition from SEC EDGAR
Source: We will use the SEC’s EDGAR system as the primary source for 10-K filings (annual financial reports). EDGAR provides public access to all filings by U.S. publicly traded companies. Specifically, we will retrieve the 10-K forms, which contain comprehensive yearly financial and business information. Key points for data acquisition:
Company Selection: For our example, we use Apple Inc. (a tech company). The approach remains the same for any company (we could later loop over all companies). Each public company is identified on EDGAR by a CIK (Central Index Key). For example, Apple’s CIK is 0000320193. We can find a company’s CIK via EDGAR’s search or a lookup API.


Retrieving Filings: Using the company’s CIK, we can query EDGAR for its annual reports. EDGAR offers a submissions API that returns a JSON of a company’s filing history, or we can scrape the HTML index. We will focus on Form 10-K filings (annual reports). For Apple, this means grabbing the 10-K for each fiscal year (e.g. 2020, 2021, … 2024). EDGAR’s JSON API at data.sec.gov/submissions/CIK##########.json lists all filings with dates, which we can filter for form type "10-K".


Download Format: Modern 10-K filings are often available as HTML (with embedded “inline XBRL” data) on EDGAR, as well as PDF. We will use the HTML versions because they contain structured data (XBRL tags) that facilitate parsing. For example, Apple’s 2024 10-K HTML filing can be fetched from EDGAR.


Automation Considerations: No login or API key is needed (EDGAR is open), but we must use a proper user agent string if scraping. The SEC also provides bulk data (e.g. a nightly bulk ZIP of all company facts) if needed for scaling. In the future, to get “all publicly listed companies,” we could use the bulk submissions data or a list of tickers from an exchange. Initially, we’ll manually specify our example company.


Why EDGAR: Using the official source ensures data accuracy and coverage. Every U.S. public company must file a 10-K yearly, which includes financial statements for the last few years, corporate information, and management commentary. The 10-K is more detailed than a glossy annual report to shareholders – it’s the authoritative source investors rely on. By pulling directly from EDGAR, we get the raw data needed for our analysis.
2. Key Financial Data to Extract (Numeric)
The financial statements in the 10-K provide the quantitative data we need to extract for each year. We will collect all essential line items from the Income Statement, Balance Sheet, and Cash Flow Statement, as well as certain derived metrics (like earnings per share) and other financial data disclosed. It’s important to be thorough – more information is better than less, since we can always filter out unneeded metrics later. We will align our extraction with standard accounting definitions (GAAP) to ensure consistency across companies. Below is a detailed list of financial data to capture for each year:
Income Statement (Profit & Loss) Metrics:


Total Revenue (Net Sales): The total sales or revenues earned by the company in that fiscal year. This is a top-line indicator of company size and growth.


Cost of Goods Sold (COGS)/Cost of Sales: The direct costs attributable to the production of the goods or services the company sells. Used to calculate gross profit.


Gross Profit: Revenue minus COGS, showing the profit after direct production costs. (If not explicitly stated, we can compute Gross Profit = Revenue – COGS.)


Operating Expenses: Key components of operating costs, typically including:


Research & Development (R&D) Expense: Particularly important for tech companies like Apple, indicating investment in innovation.


Selling, General & Administrative (SG&A) Expense: Covers marketing, overhead, and administrative costs.


(If the company reports other categories, e.g. “Marketing” separately or “Restructuring charges”, those can be captured too for completeness.)


Operating Income (Operating Profit): Profit from core business operations, i.e. gross profit minus operating expenses. This reflects earnings before interest and taxes.


Net Income (Net Profit): The bottom-line profit after all expenses, interest, and taxes. Net income is critical as it indicates overall profitability and is used in many metrics (EPS, ROE, etc.).


Earnings Per Share (EPS): Typically, both Basic and Diluted EPS for the year (if available). EPS represents net income per share of stock, a key figure for investors (the 10-K is required to report EPS).


Balance Sheet Metrics: (usually given as of the fiscal year-end date)


Cash and Cash Equivalents: The company’s cash on hand or in bank accounts. Sometimes combined with short-term marketable securities for Apple and similar companies with large cash reserves.


Current Assets: The total assets expected to be converted to cash within a year (includes cash, inventory, receivables, etc.).


Total Assets: The sum of current and long-term assets – everything the company owns of value.


Current Liabilities: Obligations due within a year (short-term debt, accounts payable, etc.).


Long-Term Debt: The amount of long-term borrowings (bonds, loans) the company has. This is a critical measure of leverage (for Apple this would include any issued bonds, etc.). We consider “debt levels” as one of the key metrics to extract. If needed, we can also capture Short-Term Debt and then compute total debt.


Total Liabilities: All liabilities (current + long-term).


Shareholders’ Equity: The book equity value (assets minus liabilities). Often broken down into common stock, retained earnings, etc., but we can take total equity.


Cash Flow Statement Metrics:


Net Cash from Operating Activities (Operating Cash Flow): Cash generated by the company’s core operations during the year. This is derived from net income with adjustments for non-cash items and working capital changes.


Net Cash from Investing Activities: Usually includes capital expenditures (money spent on property, plants, equipment) and other investments or acquisitions. The most critical component here often is Capital Expenditures (CapEx), since we may want to calculate Free Cash Flow. We should capture CapEx (often listed as “Purchase of property and equipment” or similar in cash flows).


Net Cash from Financing Activities: Cash flows from financing decisions, like issuing stock, paying dividends, borrowing or repaying debt. Key items might include Dividends Paid (to see shareholder returns) and Net Debt Issued/Repaid.


Free Cash Flow (FCF): We can compute this as Operating Cash Flow minus CapEx (it represents the cash available for debt repayment, dividends, and growth investments). It’s not explicitly listed in 10-Ks but is a crucial derived metric for valuation.


Other Financial Data/Key Figures:


Gross Margin and Operating Margin: Although these are percentages that can be calculated (Gross Margin = Gross Profit/Revenue, Operating Margin = Operating Income/Revenue), some companies report segment margins or overall gross margin. We will compute these for analysis. Apple, for instance, reports a consolidated gross margin percentage (e.g. ~46% in 2024).


Effective Tax Rate: If needed, from income tax expense vs. pre-tax income (not always critical for trend analysis, but good to note large changes).


Shares Outstanding: If available (usually in the equity section or notes), the number of shares, to verify EPS calculations or compute market ratios later.


Any industry-specific metrics: If a company has unique key metrics (e.g. bank capital ratios for banks, or subscriber counts for a telecom company reported in 10-K), those could be extracted as needed. Our focus is on financial fundamentals common to all companies for now.


By extracting all these data points for each year’s 10-K, we ensure a comprehensive dataset. These items align with standard US GAAP financial statement line items, which helps comparability. In fact, because the SEC mandates XBRL tagging of financial statements with the US-GAAP taxonomy, we can leverage that for consistent data names. The advantage is that using GAAP-standard tags means the meaning of each figure is consistent across companies and years. For example, the tag Revenue (or more precisely us-gaap:Revenues) will represent the same concept for Apple as for any other company, making cross-company comparison feasible. The SEC’s XBRL API allows pulling these standardized facts.
Extraction Method: We have two approaches for numeric data:
Preferred: Use the SEC’s XBRL Data APIs to fetch the structured data. EDGAR’s companyfacts API returns all reported financial facts for a company in JSON. We can query Apple’s data (CIK 0000320193) and retrieve values for tags like us-gaap:Revenues, us-gaap:NetIncomeLoss, etc., across all years. This approach taps directly into the data in the 10-K in a computer-friendly way. The SEC API ensures we use non-custom GAAP concepts for comparability. For instance, if we request Apple’s NetIncomeLoss, we get the net income for each year as reported, without having to scrape the text of the report. Using the API is robust to differences in formatting or wording in the filings because it relies on the underlying standardized taxonomy. (Note: We have to be mindful of cases where companies use extended/custom tags not in GAAP – but key items like revenue and net income are almost always standard tags.)


Alternative: Parse the HTML/PDF of the 10-K directly to find tables. This involves locating the financial statements within the document (e.g. the table titled “Consolidated Statements of Operations” for the income statement, etc.), then reading the numbers. We could use HTML parsing (BeautifulSoup) to find the tables by their captions or XBRL tags embedded in the HTML. However, pure HTML parsing can be challenging because each company may format tables differently or use different labels (“Net sales” vs “Total revenue”, etc.). It’s a brittle approach and requires custom handling of lots of edge cases. Therefore, we favor the XBRL/JSON method for reliability and scalability.


For each year of data we collect, we will append the results into a pandas DataFrame (or similar structure). Likely, we will structure it with one row per year, columns for each metric. For example, after processing Apple’s filings, the DataFrame might have columns: Year, Revenue, OperatingIncome, NetIncome, Assets, Equity, etc., with values for 2020, 2021, … 2024. All figures will be converted to a consistent unit (EDGAR reports are usually in millions of USD for large companies – e.g., Apple’s 2024 revenue $391,035 million, which means $391.035 billion). We’ll ensure numeric types are used so that we can easily perform calculations on them.
This comprehensive numeric dataset forms the foundation for analyzing financial performance over time.
3. Key Textual Information to Extract (Qualitative)
Apart from the numbers, 10-K reports contain a wealth of qualitative information about a company’s business, strategy, and risks. The key information we want from the text portions includes things like the company’s business model and structure, management’s insights into performance and future outlook, and major risk factors or challenges. These insights often require understanding context and nuance, so this is where we’ll employ an AI agent (LLM) to read and summarize the relevant sections. We will target the following sections of the 10-K for text extraction, based on the standard 10-K structure:
Business Overview (Item 1): This section provides an overview of the company’s operations, principal products and services, and its organizational and geographic structure. Here we will extract:


Company Description: What the company does and how it generates revenue (e.g. Apple’s business includes designing consumer electronics like iPhone, iPad, Macs, plus software and services).


Segments and Organizational Structure: Information on how the company is organized. This might include reportable business segments or divisions, or geographic segments. For example, Apple’s 10-K states that it manages its business on a geographic basis with segments Americas, Europe, Greater China, etc. We want to capture such structure information as it provides context for comparison (e.g. a company operating in multiple regions or with multiple product lines). If the 10-K explicitly lists subsidiaries or a corporate family tree (some filings include an “Organizational structure” or list of subsidiaries), we note the major subsidiaries.


Market and Competition: The business section often describes the industry and competitive landscape. We will have the AI note major competitors or competitive advantages mentioned (e.g. Apple citing competition in smartphones from Android OEMs, etc., and the basis of competition like price, innovation).


Regulatory Environment: For some industries, any key regulations or “governmental” factors affecting the business are described. (This might be what was meant by “governmental structure” in the query.) If, for example, the company is subject to specific government regulations or has a particular corporate governance structure, we will extract those details. This could include noting if the company is a regulated utility, or if it operates in a heavily regulated sector (like banking or healthcare).


Risk Factors (Item 1A): A 10-K contains a section listing the major risks that the company faces. These can range from macroeconomic and industry risks to company-specific risks (like reliance on key suppliers or IP litigation). While we do not need the full text of every risk factor (they can be very lengthy), we want to extract the key risk themes. For example, Apple’s risk factors include things like global economic conditions, supply chain dependencies, rapid industry changes, reliance on iPhone as a major product, cybersecurity, etc. We will use the AI to summarize the top risks: e.g. “Dependence on new product innovation”, “Supply chain and component availability risks”, “Competition and pricing pressure”, etc. If certain risks are particularly pertinent to future performance (like a pending legal issue or regulatory change), those will be noted. The purpose is to be aware of potential headwinds or uncertainties when comparing companies.


Selected Financial Data (Item 6, if present) or Financial Highlights: Some 10-Ks present a 5-year summary of financial data in a table. Since we are already extracting those data points directly, this section might not need separate handling. It’s essentially a quick snapshot of the numbers we’re compiling ourselves. We can skip this for text extraction, as it’s redundant to our numeric DataFrame.


MD&A – Management’s Discussion and Analysis (Item 7): This is one of the most important sections for qualitative insight. The MD&A contains management’s narrative about the financial results and the company’s strategic outlook. Here we will have the AI focus on extracting:


Performance Drivers: An explanation of why revenues, expenses, and profits changed year-over-year. For instance, management might attribute Apple’s revenue growth to higher services sales and Mac sales, but note a decline in iPad sales due to product timing. We want to capture these explanations to understand the context behind the numbers.


Future Outlook and Strategy: Critically, MD&A often includes forward-looking statements about the company’s plans, market trends, and expectations for the future. We will extract any statements about future goals, strategic initiatives, or market conditions. For example, the AI might pull out that “Apple is focused on expanding its services and wearable product categories” or “Management anticipates continued pressure on gross margins due to competitive pricing” – whatever forward-looking commentary is given. If the company provides qualitative guidance or indicates trends (even though quantitative guidance is usually in press releases, not 10-K), that will be noted. The AI should produce a concise summary of the company’s outlook and strategic priorities as described by management.


Significant Events: Any mention of notable events that year, such as acquisitions, new product launches, or other milestones. (Apple’s 10-K MD&A might mention significant product introductions or expansions into new services.) These are key to understanding how the company is evolving.


Quantitative and Qualitative Disclosures about Market Risk (Item 7A): Some 10-Ks include a section on market risk (e.g. interest rate risk, foreign exchange risk) – for Apple, currency exchange risk is a factor noted in MD&A. We may not need to separately summarize this unless it’s crucial (it often overlaps with risk factors).


Financial Statements and Notes (Item 8): The notes to financial statements can be very detailed (accounting policies, breakdown of segments, etc.). Most of the quantitative info we need comes from the main statements, which we’re extracting directly. However, some qualitative info might be buried in notes – for instance, a segment revenue breakdown or geographic revenue breakdown, or details on commitments and contingencies. If needed, we can parse certain notes: e.g., the segment reporting note which often lists revenue and profit by segment. In Apple’s case, segment info is actually summarized in MD&A text and tables (as seen above, Apple provides segment sales in the MD&A). We might rely on MD&A for such details rather than diving into XBRL segment data at first.


Management and Governance (Items 10-14): These items (executive officers, compensation, etc.) are usually incorporated by reference from the proxy statement rather than stated in full in the 10-K. Since our focus is on financial and strategic data, we will not initially extract things like executive compensation or director information from the proxy. However, if “governmental structure” was intended to mean corporate governance structure, we could note the key leadership (CEO, etc.) or any governance remarks. For example, just a short note like “CEO and CFO certify the accuracy of the 10-K” (a Sarbanes-Oxley requirement) or if the company’s ownership structure (like dual-class shares) is described. These are likely lower priority for our analysis unless needed for specific research questions. We will acknowledge where to find them but focus on the core financial and strategic content.


To extract and summarize these text sections, we will use a Large Language Model (AI agent). The process will be roughly: identify the text of each relevant section (using the HTML structure or known item headings as anchors), then feed those sections into the LLM with prompts to pull out the key information. For example, we might prompt: “Summarize the company’s business operations and competitive position” for Item 1, “What were the main factors driving changes in revenue and profit this year?” for MD&A, or “What future plans or outlook does management discuss?” for the outlook part. The AI’s output will then be parsed and added to our data structure – e.g. a dictionary of text insights or added as columns in a DataFrame (though text in DataFrame cells is fine for storage, it’s not for numeric analysis, it is useful for quick reference and qualitative analysis).
AI extraction ensures we capture nuances that simple regex or hardcoding would miss. For instance, the phrase “we expect to see improvement in margins due to cost efficiencies” requires understanding context – something an AI can summarize, whereas a regex would not know it's an outlook statement. The LLM (GPT-4 or similar) can handle the variability in how different companies write their MD&A, extracting meaning beyond exact keywords. We’ll instruct the AI to be thorough – better to over-capture insights than miss something critical. All the information we aim to extract is explicitly defined (as above), so the AI’s role is constrained to retrieving those points from the text, not inventing anything abstract.
By the end of this step, we will have a collection of qualitative insights per year – for example, for Apple 2024, a short summary of its business segments, a summary of what drove changes in that year (e.g. strong Services revenue, weaker hardware sales), what management says about the future (e.g. investing heavily in R&D for new products), and major risks (e.g. supply chain concentration in Asia, competition, etc.). These pieces of information complement the raw numbers and are crucial for a holistic analysis.
4. Assembly into Structured Format (DataFrames for Analysis)
With numeric data parsed and text summaries generated, we will compile everything into a structured format conducive to data exploration and modeling. The primary structure will be a pandas DataFrame (or a set of related DataFrames). We need to choose a format that prioritizes machine-readability and ease of analysis, rather than human readability, per the instructions. This means clear labels, consistent schema, and avoidance of free-form text where possible (aside from designated text fields).
Financial DataFrame (Numeric): This will likely be a table with one row per company per year, and columns for each financial metric. For our single-company example, it would be one row per year. If we later include multiple companies, we might add a “Company” column or use a multi-index (Company, Year). For now, imagine a table like:
Company
Year
Revenue (USD millions)
Net Income
EPS (diluted)
Total Assets
Total Liabilities
Equity
Operating Cash Flow
… (other metrics) …
Apple
2020
value
value
value
value
value
value
value
…
Apple
2021
value
value
value
value
value
value
value
…
Apple
2022
…















All numeric fields will be stored in a normalized form (e.g., actual dollar values in millions, as integers or floats). For example, Apple’s row for 2024 might show Revenue = 391035 (meaning $391,035 million), Net Income ~ 99500 ($99,500 million), etc., based on its 10-K figures. These structured data facilitate time-series analysis and can be directly fed into models or visualizations. We will ensure consistency: if a certain metric is unavailable for a given company (e.g. a company might not have R&D if not tech), we can fill with NA or 0 as appropriate, but the column will still exist for consistency across the dataset.
Textual DataFrame/Fields: The qualitative info could be stored in a separate DataFrame or merged. Since text isn’t directly usable for numeric modeling, we might keep it separate but linked by Year (and Company). One approach is to create a DataFrame with columns like “BusinessSummary”, “ManagementOutlookSummary”, “KeyRisksSummary” etc. Each cell would contain a paragraph or a set of bullet points extracted by the AI for that year. For example, BusinessSummary for Apple might be “Apple designs consumer electronics (iPhone, iPad, Mac), offers services (App Store, iCloud). Managed by geographic segments (Americas, Europe, China, etc.). Highly competitive industry with focus on innovation and ecosystem.” – all in one cell. The format here can still be machine-friendly (string data that can be further parsed or analyzed via NLP if needed). We optimize these fields for content rather than prettiness: e.g., we might store them as JSON strings with key points, or as bullet lists in text form. The key is they are easily queryable (one could search the DataFrame for keywords like “new product” or “inflation” to find if those appear in the outlook summary, for instance).
We might also consider encoding some of these qualitative insights into categorical or numeric form for modeling. For instance, a sentiment score of the MD&A, or flags for certain topics (like a dummy variable if the company mentioned “recession” or “AI” in outlook). However, that moves into analysis. In our data pipeline plan, we will at least store the raw summary text, and leave further feature engineering for later.
Format for Future Analysis: The assembled DataFrames can be easily exported as CSV files or to a SQL database. The format is tabular, which is ideal for most data analysis and machine learning workflows. Since the user specifically mentioned data exploration and modeling, having a clean CSV per company or a combined CSV for all companies/year would be useful. For example, we can output Apple_financials_10yr.csv containing the numeric data and Apple_insights_10yr.csv for the text summaries. These can be loaded into pandas or any analysis tool. The format is optimized for machine use: no unnecessary formatting, just headers and values (as opposed to, say, a PDF or Word report which is human-friendly but not parseable).
By structuring the data this way, one can easily perform operations like: time-series plotting of revenue vs net income, correlation analysis between R&D spend and revenue growth, or training a model that uses past financials and maybe sentiment of outlook to predict future performance. The structure also makes it straightforward to add additional companies (just more rows) and additional metrics (more columns) without breaking the schema.
5. Computing Performance Metrics and Trends
Once the raw data is in place, we will extend the DataFrame with derived performance metrics. These metrics distill the raw data into insights about growth, profitability, efficiency, and financial health. They are crucial for making the data actionable and for comparing companies. We will calculate and define each metric clearly. Here are important business performance metrics we will compute (each can be a new column in the DataFrame or a separate metrics table):
Year-over-Year Growth Rates: For key income statement figures:


Revenue Growth (%): Percentage change in revenue from the previous year. This shows the top-line growth trend. For example, if Apple’s revenue went from $275b to $300b, that’s a +9% growth. We’ll compute this for each year (starting from the second year since the first year in our dataset has no prior for comparison).


Net Income Growth (%): Yearly percent change in net income. This can sometimes differ significantly from revenue growth due to margin changes or one-time items.


Profitability Margins:


Gross Margin (%): Gross Profit divided by Revenue, expressed as a percentage. This indicates how efficiently a company produces its goods relative to sales. We can compute it or use any reported figure if available.


Operating Margin (%): Operating Income divided by Revenue. A core indicator of operational efficiency and cost management.


Net Profit Margin (%): Net Income divided by Revenue. This shows the overall profitability of each dollar of sales after all expenses. For instance, Apple’s net margin in recent years has been high (around 20–25%), which would be captured here.


Return Ratios:


Return on Equity (ROE): Net Income divided by average Shareholders’ Equity. This measures how effectively the company is using shareholders’ capital to generate profit. It’s a key metric for investors (higher ROE generally indicates efficient profit generation). We will use the year’s net income and that year’s equity (or an average of this and last year’s equity for precision if we have the data) to approximate ROE.


Return on Assets (ROA): Net Income divided by average Total Assets. This shows how effectively assets are being used to produce profit.


Return on Invested Capital (ROIC): (If needed) NOPAT divided by (Debt + Equity), to measure return on all capital. This is more advanced; we might skip unless needed for specific analysis.


Liquidity & Solvency:


Current Ratio: Current Assets / Current Liabilities. Measures short-term liquidity (ability to cover near-term obligations).


Debt-to-Equity Ratio: Total Debt / Equity. Indicates leverage – how much debt the company is using relative to shareholder funds. We’ll define “Total Debt” as long-term debt plus any current portion of debt. For Apple, for instance, we’d plug in its debt numbers accordingly.


Debt-to-Assets: Total Liabilities / Total Assets (or alternatively Total Debt / Total Assets). Another solvency indicator to compare across firms.


Cash Flow Metrics:


Free Cash Flow Margin: FCF / Revenue, or simply track Free Cash Flow year over year. FCF is an important indicator of how much cash the company can return to shareholders or reinvest. We might calculate a 5-year CAGR of FCF as well, for trend.


Cash Conversion or Operating Cash Flow/Net Income: This ratio shows how well accounting profits convert to cash. If OCF consistently exceeds net income, that’s generally positive (quality of earnings).


Efficiency Ratios: (if needed)


Asset Turnover: Revenue / Total Assets. Shows how efficiently the company uses assets to generate sales.


Inventory Turnover, Receivables Turnover: Possibly, if the data is available and relevant (more for internal analysis; we might skip unless focusing on operational efficiency specifically).


Each of these metrics will be clearly defined in our output so there’s no ambiguity (for example, we’ll specify if percentages are in decimal or percentage form, etc.). Computing these metrics helps interpret the raw financials. For instance, rather than just listing Apple’s revenue each year, the growth rate highlights acceleration or deceleration. Margins tell if profitability is improving or declining relative to sales. Return on equity places net income in context of investor capital – useful for cross-company comparison.
We should note that comparing these metrics across companies and time is the ultimate goal, as the user indicated. These metrics allow exactly that: one can compare, say, Apple’s net margin to Microsoft’s, or see how Apple’s ROE in 2024 compares to its ROE in 2014, or to the tech industry average. Financial analysts routinely use such metrics to gauge performance. Our pipeline will thus not only gather raw data but also present these digestible metrics.
For example, after computing metrics, we might find something like: “Apple’s revenue growth has averaged ~10% over the past 5 years, but slowed to 2% in 2024. Gross margin improved from 43% to 46% over the decade. ROE is extremely high (e.g. >50% in some years) due to strong profitability and buybacks reducing equity. Debt/Equity rose after Apple took on debt for stock buybacks, but interest coverage remains high,” etc. These insights come directly from those metrics.
We will include these in the DataFrame as new columns (e.g. RevGrowth, NetMargin, ROE, etc.), making it easy to filter or plot them. If the data will be used for modeling (machine learning), having these pre-calculated as features can be very helpful.
6. AI Agent vs. Hardcoded Logic: Division of Tasks
A critical aspect of our plan is deciding which parts of the extraction process can be handled with deterministic, rule-based code and which require the flexibility of an AI (LLM) to interpret. We will clearly delineate this to use the right tool for each job:
Hardcoded / Programmatic Extraction: This is suitable for structured, well-defined data that can be located via known patterns or structured formats. The financial statement numbers fall into this category. We can reliably locate revenue, net income, etc., either via the XBRL tags or by finding specific table structures, without needing comprehension of English sentences. By leveraging EDGAR’s structured data (XBRL JSON), we avoid the pitfalls of parsing text that might vary in wording. Hardcoding is also appropriate for repetitive web navigation tasks, like iterating through EDGAR pages or downloading multiple files, where a simple script can suffice. In short, numeric data extraction can be fully automated with code, using defined field identifiers (e.g., tags like us-gaap:OperatingIncomeLoss) rather than NLP. This approach is precise and leaves no ambiguity about what we capture.

 Additionally, if we wanted to get certain textual facts that have a clear pattern, we could hardcode those too. For example, if we wanted the “Item 1 – Business” text in full, we could find the HTML anchor for Item 1 and slice the text until Item 1A. That would give us the entire business description. However, the result would be a large chunk of text; the identification of key points within it would still need interpretation. So pure hardcode stops at extraction; interpretation needs AI (or manual analysis).


AI (LLM) Extraction: This is necessary for unstructured, semantic information – essentially the understanding and summarization of natural language sections of the 10-K. The AI agent will be used for: summarizing the business overview, extracting the essence of risk factors, explaining year-to-year changes in MD&A, and pulling out any forward-looking statements or strategic commentary. These tasks require reading comprehension at a high level. For example, management might write “Despite a challenging macroeconomic environment, we delivered record services revenue, and we continue to invest heavily in R&D to fuel future innovations”. Distilling that into “Management highlights record growth in services and ongoing heavy R&D investment despite macro headwinds” is something an AI can do by understanding the sentence. Hardcoded logic cannot infer meaning or what is “key” in such text without a pre-defined dictionary of terms. We want the AI’s ability to identify what’s important – e.g., it should recognize that “record services revenue” is a notable positive, and “challenging macro environment” implies a risk factor context, etc., and include those points in the summary. Generative AI is well-suited to handle the variety in how different companies discuss their operations and outlook.

 We will likely use a GPT-4 based agent (via OpenAI API) or a similar powerful model for the best accuracy in understanding nuance. The user mentioned possibly using an open-source model or Google’s Gemini for cost reasons. We should note that while open-source LLMs (like Llama 2) could be used to some degree, processing a full 10-K (which can be hundreds of pages) might exceed their context window or require chunking the text. GPT-4 currently has a strong track record on summarizing documents and can handle large contexts (up to 32k tokens in some versions, which might cover a big chunk of a 10-K, though possibly not all at once). If cost is a concern, we can use GPT-3.5 for initial passes (cheaper but still decent) or only summarize the most critical parts rather than everything. We can also employ strategies like splitting the document into sections (Business, MD&A, etc.) and processing sequentially to stay within context limits. Using the AI as an agent could mean having it navigate the document (with tools or via a retrieval step) and answer specific queries, which might be more efficient than a blind summary of the entire text.

 We will explicitly note for each piece of info whether it’s gathered by AI or code:


All the metrics listed in section 2 are via hardcoded logic or API (no AI needed for those).


The summaries and insights in section 3 are via AI (LLM agent reading text).


Calculations in section 5 are hardcoded (simple formulas on the data, no AI needed).


There is some overlap where either approach could work. For instance, one might try using an AI to extract financial numbers by giving it the raw filing text – but that is not necessary here since we have deterministic methods that are more accurate for that task (and we want to conserve AI usage for where it’s truly needed). Conversely, one might attempt to hardcode some text extraction by looking for certain keywords, but that runs the risk of missing context or varying phrasing. For example, a hardcoded rule might search for the word “outlook” in the MD&A and extract the following sentence, but not all companies explicitly label their forward-looking statements with “Outlook: …”. They might weave it into a paragraph. AI can catch those nuances by understanding meaning rather than exact words.
In summary, financial data extraction and calculations = code, contextual understanding and summarization = AI. This hybrid approach leverages the strengths of both: we get accuracy and consistency for structured data, and flexibility and comprehension for unstructured data. We will indicate in our documentation which fields were obtained through which method, so we know where an AI’s judgment was applied versus where it’s raw data. This can help in later validation (e.g. we might manually review the AI-generated summaries for correctness or have the AI provide references to the text so we can verify).
By structuring it this way, we minimize reliance on AI for anything that doesn’t require it (to save on costs and potential errors), and we maximize the value of AI where it’s truly beneficial (digesting complex narrative information).
7. Example Application: Apple Inc. (Tech Industry)
To illustrate how the plan comes together, let’s walk through a concrete example with Apple (while keeping it general enough to apply to any company):
Data Retrieval: We fetch Apple’s last 10 years of 10-K filings from EDGAR. For instance, Apple’s fiscal year ends in September, so the 10-K for FY2024 was filed around Oct 2024 (EDGAR Accession No. 0000320193-24-000123). We get similar filings for 2023, 2022, and so on back to 2015 (or 2010 if we want a full decade). Each is downloaded as an HTML file with inline XBRL.


Numeric Extraction via XBRL: Using the SEC’s API, we request Apple’s financial facts. For example, call the companyfacts API for Apple’s CIK. From the JSON, extract the annual facts for key tags:


us-gaap:Revenues – yields revenue for 2015 through 2024. (We’d find, for instance, $233,715 million in 2015 rising to $391,035 million in 2024.)


us-gaap:NetIncomeLoss – yields net income each year (e.g., $53,394M in 2015, $94,680M in 2021, etc., and $99,803M in 2022, $94,680M in 2023 – Apple’s net income dipped slightly in 2023, etc.).


us-gaap:DilutedEPS – yields EPS values (e.g., for 2024 around $6.05/share if we calculate from net income ~$99B and share count ~16.1B, but the exact figure can be pulled).


us-gaap:ResearchAndDevelopmentExpense – to get R&D (Apple’s 2024 R&D was $31,370M). Similarly SG&A (us-gaap:SellingGeneralAndAdministrativeExpense = $26,097M in 2024).


us-gaap:CashAndCashEquivalentsAtCarryingValue (and maybe marketable securities) for cash; us-gaap:LongTermDebt for debt (Apple had about $98B long-term debt in 2024), etc.


We continue this for all required fields. The API returns data in a structured way (each concept includes an array of values with periods). We filter for annual periods (e.g. “2024-09-30” for Apple’s FY end date context) and build our DataFrame row.


Text Extraction via AI: We feed the relevant sections of Apple’s 10-K to GPT-4 (or similar). For example:


Business Overview: The AI reads that Apple’s main business is designing and selling smartphones, personal computers, tablets, wearables, and services. It notes Apple’s segments by geography and that it has a broad global distribution network, etc. The output might be a concise summary: “Apple Inc. designs and manufactures consumer electronics (iPhone, iPad, Mac), along with software and services (App Store, Apple Music, iCloud). It operates worldwide, with major segments in the Americas, Europe, Greater China, Japan, and Rest of Asia Pacific. Apple’s competitive advantage is its integrated ecosystem and brand, but it faces intense competition in all product categories, often with aggressive pricing by rivals.” This text (sourced from the 10-K content) would populate our BusinessSummary field for Apple.


MD&A Performance & Outlook: The AI reads Item 7. For Apple 2024, it finds management discussing that 2024 net sales grew 2% (from $383.3B to $391.0B) with growth in Services and certain hardware like Mac, but declines in iPad and accessories. It notes statements that foreign exchange headwinds (strong dollar) hurt international sales growth. It sees mention that Apple continues to invest in R&D for future products and that gross margin improved to 46% due to cost savings. The AI also looks for future outlook indicators: Apple might not give explicit guidance in the 10-K, but management might say they expect certain trends (e.g. “The Company expects its quarterly net sales and results of operations to fluctuate” due to seasonality). The AI might summarize the outlook as: “Management remains optimistic about long-term opportunities, citing strength in Services and upcoming product innovation, but notes near-term headwinds like foreign currency impacts and soft demand in some hardware segments. They emphasize continued investment in R&D and future product pipeline.” This would fill something like an OutlookSummary for that year.


Key Risks: The AI reads Item 1A and picks out high-level risks. For Apple: dependence on iPhone (a significant portion of sales comes from one product), global supply chain concentration (many components sourced from Asia, subject to disruption), competition and pricing pressure, regulatory risks (antitrust, digital markets act in EU as referenced in their filings), and macroeconomic risks (inflation, consumer spending). It might produce a summary: “Key risks include: heavy reliance on iPhone for revenue (a demand decline in this product could significantly impact sales); global economic and forex fluctuations which can reduce consumer demand; supply chain disruptions or component shortages (many suppliers in Asia); intense competition in tech devices and services leading to pressure on margins; and regulatory/legal challenges (antitrust scrutiny, privacy regulations) that could force business changes or fines.” We store that in RiskSummary.


DataFrame assembly: For Apple, we now have (for each year 2015–2024) a row of numbers and a few text fields. We confirm consistency (for instance, check that the revenue numbers from XBRL match those mentioned in the MD&A text – they should). We then compute metrics: e.g., fill a column RevenueGrowth with values like Apple 2024: +2% (since $391B vs $383B in 2023), 2023: -3% (383 vs 394 in 2022), etc.. Net margin for 2024: ~$90B net / $391B = ~23%. ROE: Apple’s net income ~$94B vs equity ~$64B (Apple’s equity is low because of stock buybacks), giving an extremely high ROE ~147% – reflecting its capital structure. These calculations are automated. We might also have a 5yr Revenue CAGR for the period or other multi-year stats if needed.


Example Insights: With the compiled data, we could already glean some insights: Apple’s revenue nearly doubled from 2015 to 2024, but growth has slowed recently (2% in 2024). Net income grew strongly and margins have been fairly stable or improving (gross margin up to 46%). Apple’s R&D has grown significantly (from ~$8B in 2015 to $31B in 2024), indicating a strategic focus on new products. The AI summaries tell us Apple is pushing into services (now ~25% of revenue) and sees that as a growth driver (since the AI noted “record services revenue”). Risks summaries highlight things like reliance on one product and supply chain, which are important context when comparing to, say, Microsoft (which might be more diversified in revenue sources) or other peers.


This example confirms that our defined information captures the essential story: how Apple is performing financially, where it’s headed, and what challenges it faces – all in a structured format. We can replicate this for any company’s 10-K because the plan is based on general 10-K content requirements (per SEC regulations, every 10-K must cover the areas we listed, even though the specifics differ by company).
8. Scaling Up and Comparing Across Companies
Although initially we focus on one company, the ultimate aim is to scale this to multiple companies (industry-wide) and perform comparative trend analysis. Our plan sets the stage for that:
Repeating the Pipeline: The scraping and extraction steps can be looped over a list of companies. We could obtain a list of all companies in an industry by SIC code or an index (for example, all tech hardware companies). EDGAR’s API or bulk data can provide all company CIKs and their sectors (the submissions JSON includes SIC code which indicates industry). We can filter, say, for SIC 3571 (Electronic Computers) to get PC/smartphone makers, etc., or simply maintain a list of peer companies (e.g., Samsung if it were listed in US, but it’s not; so maybe Microsoft, Google (Alphabet), etc., for Apple’s peers – though Google files a 10-K as Alphabet, Microsoft as itself, etc.). We would run the same steps for each, populating a combined dataset.


Data Alignment: Because we standardized on GAAP metrics and used consistent column names, combining data from multiple companies is straightforward. Each company’s DataFrame can be concatenated into one big DataFrame with an added “Company” column. The metrics are directly comparable (e.g., Revenue for Apple vs Microsoft – both in millions USD; Net Income margin percentages, etc.). Some caution: fiscal year timing differences (Apple’s FY vs calendar year for others) but we can align by calendar year for analysis if needed.


Trend and Benchmark Analysis: With the multi-company data, we can compute industry aggregates or compare ranks. For example, we can add an Industry Average row (or compute it on the fly) by averaging peer metrics for each year. This would highlight, say, if Apple’s revenue growth is above or below industry average, or whose margins are higher. We can also track trends: maybe all companies show margin compression in a certain year due to a common factor (like component cost increases); our data would show that.


Visualization and Modeling: The structured format allows easy plotting (e.g., time-series line charts of each company’s revenue, bar charts of their ROE side by side, etc.). For modeling, one could feed the data into regression models or machine learning algorithms to predict future performance or stock returns, etc. The textual summaries could be vectorized (using NLP techniques) to include qualitative features (for instance, sentiment score of the outlook, or frequency of certain keywords like “uncertain” or “opportunity” in MD&A). This could be part of a quant model to see if certain language predicts future stock performance – just an example of advanced use.


Example Comparison: If we had done this for, say, Apple, Microsoft, Google, and Amazon, we could compare how their key metrics evolved over 2015–2024. We’d see Apple and Microsoft’s margins versus Amazon’s (Amazon has thin margins), or R&D as a percent of revenue (Apple ~8%, Google ~15%, etc.). The AI text might reveal different strategic focuses: Apple focused on hardware-software integration, Google on advertising and AI, etc., which an analyst could use to qualitatively explain the quantitative differences.


Finally, we ensure every piece of information we defined is indeed captured. If during research we found additional data points that should be included (for completeness or because “more is better than less”), we incorporate them. The plan as outlined covers the major financial and textual aspects of a 10-K as per SEC requirements, which should suffice for a robust analysis dataset.
All these elements will be documented and defined in our output, so nothing is abstract. For instance, if we have a column “OperatingIncome”, we will define it clearly (income from operations, as per Income Statement, before interest and taxes). If we have “OutlookSummary”, we clarify it contains forward-looking statements extracted from MD&A. This way, any user of the data or future developer knows exactly what each field represents and how it was obtained.
In summary, this plan lays out a structured approach to go from raw 10-K filings to a rich dataset of financial history and insights. By following GAAP standards and leveraging EDGAR’s data, we get accurate numbers; by using AI for the narrative sections, we get nuanced information that would be hard to code; and by combining these, we create a powerful tool for quantitative analysis and modeling of company performance over time. Investors and analysts can then use this compiled data to make informed comparisons, detect trends (like how tech companies’ margins are trending, or how a firm’s strategy shifts over years), and potentially predict future outcomes. This aligns with the end-goal of performing quantitative exploration and trend analysis across companies and industries, built on a foundation of thoroughly extracted data.
Sources: The plan above is informed by SEC guidelines and common financial analysis practices. SEC’s Investopedia summary highlights the sections of a 10-K and what’s included. The SEC EDGAR API documentation confirms the availability of structured data using the US-GAAP taxonomy for consistency across filings. Data extraction best practices suggest focusing on key metrics like revenue, net income, EPS, cash flow, debt, etc.. Key sections like Business and MD&A provide insights into company goals and outlook. Finally, deriving metrics (margins, returns) allows comparing a company’s performance to its past and to peers, which is exactly the purpose of our project. We have ensured every item of information to collect is explicitly identified and justified by these sources and standard analysis needs. This concludes the detailed structure for the project.

