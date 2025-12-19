import smtplib
import csv
import time
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from string import Template
import yaml
from datetime import datetime
from pathlib import Path
from smtplib import SMTPServerDisconnected, SMTPAuthenticationError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('newsletter.log'),
        logging.StreamHandler()
    ]
)

class NewsletterSender:
    # class: NewsletterSender
    def __init__(self, config_path='config.yml'):
        print("Initializing NewsletterSender...")
        self.config = self._load_config(config_path)
        self.sent_count = 0
        self.last_send_time = 0
        self.last_successful_email = None  # Track last successful email
        self.stop_on_error = True  # Flag to control stopping on errors
        print("Initialization complete.")
    
    def _load_config(self, config_path):
        """Load SMTP and sending configuration from YAML file"""
        print(f"Loading configuration from {config_path}...")
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        print("Configuration loaded successfully.")
        return config
    
    def _read_template(self, template_path):
        """Read HTML template file"""
        with open(template_path, 'r', encoding='utf-8') as f:
            return Template(f.read())
    
    def _read_recipients(self, csv_path):
        """Read recipient data from CSV file"""
        recipients = []
        with open(csv_path, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                recipients.append(row)
        return recipients
    
    def _rate_limit(self):
        """Implement rate limiting to avoid spam filters"""
        if self.sent_count >= self.config['rate_limit']['emails_per_batch']:
            batch_delay = self.config['rate_limit']['batch_delay']
            print(f"\nBatch limit reached. Waiting {batch_delay} seconds...")
            
            for remaining in range(batch_delay, 0, -1):
                print(f"\rResuming in {remaining} seconds...  ", end='', flush=True)
                time.sleep(1)
            print("\rResuming now!                           ")
            
            self.sent_count = 0
        else:
            current_time = time.time()
            time_since_last = current_time - self.last_send_time
            if time_since_last < self.config['rate_limit']['delay_between_emails']:
                wait_time = self.config['rate_limit']['delay_between_emails'] - time_since_last
                print(f"\rWaiting {wait_time:.1f} seconds...", end='', flush=True)
                time.sleep(wait_time)
                print("\r                           ", end='', flush=True)
    
    def _test_smtp_connection(self):
        """Test SMTP connection before sending batch emails"""
        try:
            print("Attempting SSL connection to SMTP server...")
            with smtplib.SMTP_SSL(self.config['smtp']['host'], self.config['smtp']['port']) as server:
                server.login(self.config['smtp']['username'], self.config['smtp']['password'])
                logging.info("SMTP connection test successful")
                print("SMTP connection test successful!")
                return True
        except SMTPAuthenticationError:
            logging.error("SMTP authentication failed. Please check your credentials.")
            raise
        except Exception as e:
            logging.error(f"SMTP connection test failed: {str(e)}")
            raise
    
    def _read_blacklist(self, blacklist_path):
        """Read blacklist CSV into a lowercase set with file locking and robust parsing."""
        import os
        blacklisted = set()
        try:
            # Resolve file size for locking
            file_size = os.path.getsize(blacklist_path)

            # Open and lock for read to avoid concurrent write inconsistencies
            with open(blacklist_path, 'r', encoding='utf-8', newline='') as f:
                # Windows file locking
                if os.name == 'nt':
                    try:
                        import msvcrt
                        # Acquire a read lock on the whole file
                        msvcrt.locking(f.fileno(), msvcrt.LK_RLCK, file_size if file_size > 0 else 1)
                    except Exception as lock_err:
                        logging.warning(f"Could not acquire read lock on blacklist file: {lock_err}")

                # Parse CSV; support both headered and headerless formats robustly
                # Prefer DictReader for 'email' header; fallback to plain reader
                peek = f.readline()
                f.seek(0)
                has_header = 'email' in (peek or '').lower()

                if has_header:
                    reader = csv.DictReader(f)
                    for row in reader:
                        email = (row.get('email') or '').strip().lower()
                        if email:
                            blacklisted.add(email)
                else:
                    reader = csv.reader(f)
                    for row in reader:
                        if not row:
                            continue
                        email = (row[0] or '').strip().lower()
                        # Skip a stray header line
                        if email == 'email':
                            continue
                        if email:
                            blacklisted.add(email)

                # Unlock on Windows
                if os.name == 'nt':
                    try:
                        import msvcrt
                        msvcrt.locking(f.fileno(), msvcrt.LK_UNLCK, file_size if file_size > 0 else 1)
                    except Exception as unlock_err:
                        logging.warning(f"Could not release read lock on blacklist file: {unlock_err}")

        except FileNotFoundError:
            logging.error(f"Blacklist file not found: {blacklist_path}")
            raise
        except Exception as e:
            logging.error(f"Error reading blacklist file '{blacklist_path}': {e}")
            raise

        logging.info(f"Loaded {len(blacklisted)} blacklisted addresses")
        return blacklisted
    
    def send_newsletters(self, template_path, csv_path, stop_on_error=True, blacklist_path=None):
        """Main method to send newsletters to all recipients"""
        self.stop_on_error = stop_on_error
        print("\n=== Starting Newsletter Sending Process ===")
        print(f"Template: {template_path}")
        print(f"Recipients: {csv_path}")
        print(f"Stop on error: {stop_on_error}")

        # Load blacklist (fail closed if missing/unreadable)
        if blacklist_path is None:
            blacklist_path = r"C:\Users\Contatto\Desktop\newsletter-sender\recipients_blacklist.csv"
        print(f"Blacklist: {blacklist_path}")
        try:
            blacklist_emails = self._read_blacklist(blacklist_path)
        except Exception as e:
            print(f"\n❌ Could not read blacklist: {e}")
            logging.error(f"Failed to load blacklist from {blacklist_path}: {e}")
            print("🛑 Aborting send to avoid emailing blacklisted recipients.")
            return

        # Test SMTP connection first
        print("\nTesting SMTP connection...")
        self._test_smtp_connection()
        
        template = self._read_template(template_path)
        recipients = self._read_recipients(csv_path)
        total_recipients = len(recipients)
        print(f"\nFound {total_recipients} recipients to process")
        
        # Create results directory if it doesn't exist
        results_dir = Path('results')
        results_dir.mkdir(exist_ok=True)
        
        # Create results file
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        results_file = results_dir / f'sending_results_{timestamp}.csv'
        print(f"Results will be saved to: {results_file}")
        
        with open(results_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['email', 'status', 'error_message'])
            
            try:
                print("\nConnecting to SMTP server...")
                with smtplib.SMTP_SSL(self.config['smtp']['host'], self.config['smtp']['port']) as server:
                    server.login(self.config['smtp']['username'], self.config['smtp']['password'])
                    print("Connected successfully!")
                    
                    for index, recipient in enumerate(recipients, 1):
                        # Blacklist check (case-insensitive)
                        recipient_email = (recipient.get('email') or '').strip()
                        recipient_email_lc = recipient_email.lower()
                        if recipient_email_lc in blacklist_emails:
                            print(f"\nSkipping {index}/{total_recipients}: {recipient_email} (blacklisted)")
                            logging.warning(f"Skipped blacklisted recipient at {datetime.now().isoformat()}: {recipient_email}")
                            writer.writerow([recipient_email, 'skipped_blacklist', 'blacklisted'])
                            # Do not update counters; continue to next recipient
                            continue

                        print(f"\nProcessing {index}/{total_recipients}: {recipient_email}")
                        retries = 3
                        email_sent_successfully = False
                        non_fatal_skip = False  # track skips like 556 and policy violation
                        
                        for attempt in range(retries):
                            try:
                                if attempt > 0:
                                    print(f"Retry attempt {attempt + 1}/{retries}")
                                
                                self._rate_limit()
                                
                                # Create email
                                msg = MIMEMultipart('alternative')
                                msg['Subject'] = self.config['email']['subject']
                                msg['From'] = self.config['email']['from']
                                msg['To'] = recipient['email']
                                
                                # Add HTML content
                                with open(template_path, 'r', encoding='utf-8') as f:
                                    html = f.read()
                                msg.attach(MIMEText(html, 'html'))
                                
                                # Send email
                                print("Sending email...", end=' ', flush=True)
                                server.send_message(msg)
                                print("✓ Sent!")
                                
                                logging.info(f"Successfully sent email to {recipient['email']}")
                                writer.writerow([recipient['email'], 'success', ''])
                                
                                self.sent_count += 1
                                self.last_send_time = time.time()
                                self.last_successful_email = recipient['email']
                                email_sent_successfully = True
                                break
                                
                            except SMTPServerDisconnected:
                                if attempt < retries - 1:
                                    print(f"Connection lost, retrying in 5 seconds...")
                                    time.sleep(5)
                                    print("Reconnecting to SMTP server...")
                                    server = smtplib.SMTP_SSL(self.config['smtp']['host'], self.config['smtp']['port'])
                                    server.login(self.config['smtp']['username'], self.config['smtp']['password'])
                                else:
                                    raise
                            except smtplib.SMTPRecipientsRefused as e:
                                # Robustly extract code/message from refused recipients (tuple may have 2 or 3 elements)
                                code = None
                                message = ""
                                recipients_info = getattr(e, "recipients", {})
                                if isinstance(recipients_info, dict) and recipients_info:
                                    _, info = next(iter(recipients_info.items()))
                                    if isinstance(info, tuple) and len(info) >= 1:
                                        code = info[0]
                                        raw_msg = info[1] if len(info) > 1 else ""
                                        message = raw_msg.decode() if isinstance(raw_msg, (bytes, bytearray)) else str(raw_msg)
                                    else:
                                        message = str(info)
                                else:
                                    message = str(e)
                                
                                # Skip invalid destination domains
                                if code == 556:
                                    print("❌ Error 556: invalid destination domain. Skipping and continuing.")
                                    logging.warning(f"Skipped {recipient['email']}: {code} {message}".strip())
                                    writer.writerow([recipient['email'], 'skipped', f'{code} {message}'.strip()])
                                    non_fatal_skip = True
                                    break
                                
                                # Pause on provider policy violation, then continue to next recipient
                                if 'policy violation' in (message or '').lower():
                                    print("⚠️ Policy violation detected. Pausing 10 minutes, then continuing.")
                                    logging.warning(f"Policy violation for {recipient['email']}: {code} {message}".strip())
                                    writer.writerow([recipient['email'], 'skipped_policy_violation', f'{code} {message}'.strip()])
                                    time.sleep(600)
                                    non_fatal_skip = True
                                    break
                                
                                print(f"❌ Error: {code} {message}".strip())
                                logging.error(f"Failed to send to {recipient['email']}: {code} {message}".strip())
                                writer.writerow([recipient['email'], 'failed', f'{code} {message}'.strip()])
                                
                                if self.stop_on_error:
                                    print(f"\n🛑 STOPPING DUE TO ERROR!")
                                    print(f"📧 Last successful email sent to: {self.last_successful_email}")
                                    print(f"📊 Total emails sent successfully: {self.sent_count}")
                                    print(f"❌ Failed on email: {recipient['email']}")
                                    print(f"💥 Error details: {code} {message}".strip())
                                    return
                                break
                            except smtplib.SMTPDataError as e:
                                # Handle data errors with explicit smtp_code
                                code = getattr(e, "smtp_code", None)
                                msg = getattr(e, "smtp_error", "")
                                message = msg.decode() if isinstance(msg, (bytes, bytearray)) else str(msg)
                                
                                # Skip invalid destination domains
                                if code == 556:
                                    print("❌ Error 556: invalid destination domain. Skipping and continuing.")
                                    logging.warning(f"Skipped {recipient['email']}: {code} {message}".strip())
                                    writer.writerow([recipient['email'], 'skipped', f'{code} {message}'.strip()])
                                    non_fatal_skip = True
                                    break
                                
                                # Pause on provider policy violation, then continue
                                if 'policy violation' in (message or '').lower():
                                    print("⚠️ Policy violation detected. Pausing 10 minutes, then continuing.")
                                    logging.warning(f"Policy violation for {recipient['email']}: {code} {message}".strip())
                                    writer.writerow([recipient['email'], 'skipped_policy_violation', f'{code} {message}'.strip()])
                                    time.sleep(600)
                                    non_fatal_skip = True
                                    break
                                
                                print(f"❌ Error: {code} {message}".strip())
                                logging.error(f"Failed to send to {recipient['email']}: {code} {message}".strip())
                                writer.writerow([recipient['email'], 'failed', f'{code} {message}'.strip()])
                                
                                if self.stop_on_error:
                                    print(f"\n🛑 STOPPING DUE TO ERROR!")
                                    print(f"📧 Last successful email sent to: {self.last_successful_email}")
                                    print(f"📊 Total emails sent successfully: {self.sent_count}")
                                    print(f"❌ Failed on email: {recipient['email']}")
                                    print(f"💥 Error details: {code} {message}".strip())
                                    return
                                break
                            except Exception as e:
                                error_msg = str(e)
                                
                                # Catch-all: pause and continue on policy violation text
                                if 'policy violation' in error_msg.lower():
                                    print("⚠️ Policy violation detected. Pausing 10 minutes, then continuing.")
                                    logging.warning(f"Policy violation for {recipient['email']}: {error_msg}")
                                    writer.writerow([recipient['email'], 'skipped_policy_violation', error_msg])
                                    time.sleep(600)
                                    non_fatal_skip = True
                                    break
                                
                                print(f"❌ Error: {error_msg}")
                                logging.error(f"Failed to send to {recipient['email']}: {error_msg}")
                                writer.writerow([recipient['email'], 'failed', error_msg])
                                
                                if self.stop_on_error:
                                    print(f"\n🛑 STOPPING DUE TO ERROR!")
                                    print(f"📧 Last successful email sent to: {self.last_successful_email}")
                                    print(f"📊 Total emails sent successfully: {self.sent_count}")
                                    print(f"❌ Failed on email: {recipient['email']}")
                                    print(f"💥 Error details: {error_msg}")
                                    return
                                break
                        
                        # Do NOT stop if it was a non-fatal skip (like 556 or policy violation)
                        if not email_sent_successfully and self.stop_on_error and not non_fatal_skip:
                            print(f"\n🛑 STOPPING DUE TO FAILED EMAIL AFTER ALL RETRIES!")
                            print(f"📧 Last successful email sent to: {self.last_successful_email}")
                            print(f"📊 Total emails sent successfully: {self.sent_count}")
                            print(f"❌ Failed on email: {recipient['email']}")
                            return
                    
                print("\n=== Newsletter Sending Process Complete ===")
                print(f"📧 Last successful email sent to: {self.last_successful_email}")
                print(f"📊 Total emails sent successfully: {self.sent_count}")
                
            except Exception as e:
                print(f"\n❌ Fatal Error: {str(e)}")
                print(f"📧 Last successful email sent to: {self.last_successful_email}")
                print(f"📊 Total emails sent successfully: {self.sent_count}")
                logging.error(f"SMTP connection error: {str(e)}")
                raise

def main():
    try:
        print("Starting newsletter sending script...")
        sender = NewsletterSender()
        # You can set stop_on_error=False if you want to continue despite errors
        sender.send_newsletters('template.html', 'recipients.csv', stop_on_error=True)
        print("\nScript completed successfully!")
    except Exception as e:
        print(f"\nScript failed with error: {str(e)}")
        raise

if __name__ == "__main__":
    main()
