"""Initialize database schema."""
import sys
import os

# Add app to path
sys.path.insert(0, '/app')
os.chdir('/app')

# Now import after path is set
from app.config.database import DatabaseManager
from app.models import Base

def main():
    """Create all database tables."""
    print("Creating database tables...")
    
    db = DatabaseManager()
    Base.metadata.create_all(db.engine)
    
    print("✓ All tables created successfully!")
    
    # Print created tables
    print("\nCreated tables:")
    for table in Base.metadata.sorted_tables:
        print(f"  - {table.name}")

if __name__ == "__main__":
    main()
