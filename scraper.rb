require 'nokogiri'
require 'open-uri'
require 'sqlite3'
require 'logger'
require 'date'
require 'cgi'

# Set up a logger to log the scraped data
logger = Logger.new(STDOUT)

# Step 1: Initialize the SQLite database
db = SQLite3::Database.new "data.sqlite"

logger.info("Create table")
# Create table
db.execute <<-SQL
  CREATE TABLE IF NOT EXISTS southernmidlands (
    id INTEGER PRIMARY KEY,
    description TEXT,
    date_scraped TEXT,
    date_received TEXT,
    on_notice_to TEXT,
    address TEXT,
    council_reference TEXT,
    applicant TEXT,
    owner TEXT,
    stage_description TEXT,
    stage_status TEXT,
    document_description TEXT,
    title_reference TEXT
  );
SQL

# Define methods first
def scrape_job_details(url, db, logger)
  # Initialize variables within the method to ensure they are passed correctly
  address = ''  
  description = ''
  on_notice_to = ''
  title_reference = ''
  date_received = ''
  council_reference = ''
  applicant = ''
  owner = ''
  stage_description = ''
  stage_status = ''
  document_description = ''
  
  job_page = Nokogiri::HTML(open(url))

  # Extract the "Posted" date from the <p class="subdued"> tag
  job_page.css('p.subdued').each do |p|
    logger.info("Found p.subdued content: #{p.text}")  # Log the full content of the <p class="subdued"> tag
  
    # Adjust the regex to capture the date in the format "20 February 2025"
    date_received_match = p.text.match(/(\d{1,2}\s+[A-Za-z]+\s+\d{4})(?=,)/)
  
    if date_received_match
      date_received_str = date_received_match[1]
      # Convert to Date object and reformat to "YYYY-MM-DD"
      date_received = Date.parse(date_received_str).strftime('%Y-%m-%d')
  
      # Calculate the "on_notice_to" date as 14 days after the "date_received"
      on_notice_to = (Date.parse(date_received) + 14).strftime('%Y-%m-%d')
  
      # Log the date received and on_notice_to
      logger.info("date_received: #{date_received}")
      logger.info("on_notice_to: #{on_notice_to}")
    else
      logger.error("No date found for job.")
    end
  end

  # Extract the location and proposal information from the <p> tags
  job_page.css('p').each do |p|
    # Only process <p> tags that start with "Location:"
    next unless p.text.start_with?("Location:")

    location_match = p.text.match(/Location:\s*(.*?)(?=\s*Proposal)/)
    proposal_match = p.text.match(/Proposal:\s*(.*)/)
    proposal = proposal_match ? proposal_match[1].strip : 'Proposal not found'
    
    pdf_link_match = p.at('a.pdf')['href'] if p.at('a.pdf')

    # Extract the details if they exist
    address = location_match ? location_match[1].strip : 'Address not found'
    document_description = pdf_link_match ? "https://www.southernmidlands.tas.gov.au" + pdf_link_match : 'No PDF link'

    # Clean up the proposal for council_reference and description
    council_reference = proposal.split(' ')[0].strip  # Extract the full DA reference, e.g., DA2400094
    
    # Extract everything after the last digit (which should be the description)
    description_match = proposal.match(/(\d+)\s*(.*)/)
    description = description_match ? description_match[2].strip : 'Description not found'

    # Remove the "View Application" part from the proposal string
    proposal = council_reference.gsub("View Application", "").strip

    # Log the data
    logger.info("Address: #{address}")
    logger.info("Council Reference: #{council_reference}")
    logger.info("Description: #{description}")
    logger.info("Description: #{proposal}")
    logger.info("PDF Link: #{document_description}")
  end
  
  # Step 3: Save data to the database
  save_to_database(address, council_reference, description, document_description, date_received, on_notice_to, db, logger)
end

def save_to_database(address, council_reference, description, document_description, date_received, on_notice_to, db, logger)
  # Ensure no duplicate entries
  existing_entry = db.execute("SELECT * FROM southernmidlands WHERE council_reference = ?", council_reference)

  if existing_entry.empty?  # Only insert if the entry doesn't already exist
    db.execute("INSERT INTO southernmidlands (address, council_reference, description, document_description, date_received, on_notice_to, date_scraped)
                VALUES (?, ?, ?, ?, ?, ?, ?)", [address, council_reference, description, document_description, date_received, on_notice_to, Date.today.to_s])
    logger.info("Data for job with Reference #{council_reference} saved to database.")
  else
    logger.info("Duplicate entry for job Reference #{council_reference}. Skipping insertion.")
  end
end

# URL of the Southern Midlands Council planning applications page
url = "https://www.southernmidlands.tas.gov.au/advertised-development-applications/"

# Step 2: Fetch the page content
begin
  logger.info("Fetching page content from: #{url}")
  page_html = open(url).read
  logger.info("Successfully fetched page content.")
rescue => e
  logger.error("Failed to fetch page content: #{e}")
  exit
end

# Step 3: Parse the page content using Nokogiri
main_page = Nokogiri::HTML(page_html)

logger.info("Start Extraction of Data")

# Find all <a> tags inside the <article> tags and get their hrefs
main_page.css('article .content h2 a').each do |link|
  job_url = "https://www.southernmidlands.tas.gov.au" + link['href']  # Complete URL for the job
  logger.info("Found job link: #{job_url}")

  # Now you would call the scrape_job_details method to extract the job data
  scrape_job_details(job_url, db, logger)
end
