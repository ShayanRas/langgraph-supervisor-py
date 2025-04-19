import os
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

# Get the database URL from environment variables
DATABASE_URL = os.getenv("SUPABASE_DB_URL")

if not DATABASE_URL:
    logger.error("SUPABASE_DB_URL environment variable not set.")
    raise ValueError("SUPABASE_DB_URL environment variable not set.")

engine = None
SessionLocal = None

try:
    # Create the SQLAlchemy engine
    # pool_recycle=3600 helps manage connections over time
    # pool_pre_ping=True checks connection validity before use
    engine = create_engine(
        DATABASE_URL,
        pool_recycle=3600, 
        pool_pre_ping=True
    )
    
    # Test the connection
    with engine.connect() as connection:
        logger.info("Database connection successful.")

    # Create a configured "Session" class
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    logger.info("SQLAlchemy SessionLocal created successfully.")

except OperationalError as e:
    logger.error(f"Database connection failed: {e}")
    # Depending on the application structure, you might want to exit or handle this differently
    raise OperationalError(f"Could not connect to the database: {e}") from e
except Exception as e:
    logger.error(f"An unexpected error occurred during database setup: {e}")
    raise

# Dependency function to get a DB session
# Use this in your FastAPI routes or other functions that need a session
def get_db():
    if not SessionLocal:
        logger.error("SessionLocal not initialized. Database connection might have failed.")
        raise RuntimeError("Database session factory not initialized.")
        
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Example of using the session (optional, for testing)
if __name__ == "__main__":
    db_gen = None # Initialize to None
    try:
        db_gen = get_db()
        db = next(db_gen)
        logger.info("Successfully obtained a database session.")
        # You can perform a simple query here to test further
        result = db.execute(text("SELECT 1"))
        logger.info(f"Database query 'SELECT 1' result: {result.scalar()}")
        logger.info("Database connection test successful!")
    except OperationalError as oe:
        logger.error(f"Database connection failed during test: {oe}")
    except Exception as e:
        logger.error(f"Failed to get database session or query during test: {e}")
    finally:
        # Ensure the session is closed if get_db() was successful
        if db_gen is not None:
            try:
                next(db_gen) # Trigger the finally block in get_db to close session
            except StopIteration:
                pass # Expected behavior
            except Exception as e:
                logger.error(f"Error closing database session during cleanup: {e}")