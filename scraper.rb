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

	# Define variables for storing extracted data for each entry
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
	date_scraped = Date.today.to_s


	logger.info("Start Extraction of Data")

# Define methods first
def scrape_job_details(url, db, logger)
  job_page = Nokogiri::HTML(open(url))

  # Extract the location and proposal information from the <p> tag
  job_page.css('p').each do |p|
    # Only process <p> tags that start with "Location:"
    next unless p.text.start_with?("Location:")

    location_match = p.text.match(/Location:\s*(.*?)(?=\s*Proposal)/)
    proposal_match = p.text.match(/Proposal:\s*(.*)/)
    pdf_link_match = p.at('a.pdf')['href'] if p.at('a.pdf')

    # Extract the details if they exist
    address = location_match ? location_match[1].strip : nil
    proposal = proposal_match ? proposal_match[1].strip : nil
    document_description = pdf_link_match ? "https://www.southernmidlands.tas.gov.au" + pdf_link_match : nil

    # Clean up the proposal for council_reference and description
    council_reference = proposal.sub(/^DA/, '').strip  # Remove 'DA' and trim the rest
    description = proposal.include?("Dwelling") ? "Dwelling" : proposal.split(' ').last  # Simplified description (could be further refined)

    # Remove the "View Application" part from the proposal string
    description = description.gsub("View Application", "").strip
	  
    # Log the data
    logger.info("Location: #{address}")
    logger.info("Proposal: #{council_reference}")
    logger.info("Description: #{description}")
    logger.info("PDF Link: #{document_description}")

    # Step 3: Save data to the database
    save_to_database(address, council_reference, description, document_description, db, logger)
  end
end

def save_to_database(address, council_reference, description, document_description, db, logger)
  # Ensure no duplicate entries
  existing_entry = db.execute("SELECT * FROM southernmidlands council_reference = ?", council_reference)

  if existing_entry.empty?  # Only insert if the entry doesn't already exist
    db.execute("INSERT INTO southernmidlands (address, council_reference, description, document_description, date_scraped)
                VALUES (?, ?, ?, ?, ?)", [address, council_reference, description, document_description, Date.today.to_s])
    logger.info("Data for job with location #{address} saved to database.")
  else
    logger.info("Duplicate entry for job at #{address}. Skipping insertion.")
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
