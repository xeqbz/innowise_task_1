import argparse
from database import Database
from queries import QueryExecutor
from data_loader import DataLoader
import logging
import os

logger = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(description="Insert data in database")
    parser.add_argument('--students', required=True, help="Path to students JSON file")
    parser.add_argument('--rooms', required=True, help="Path to rooms JSON file")
    parser.add_argument('--format', choices=['xml', 'json'], default='json', help="Output format(JSON or XML)")
    args = parser.parse_args()

    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)
    file_ext = 'json' if args.format.lower() == 'json' else 'xml'

    db = Database('config/config.ini')
    try:
        db.connect()
        query_executor = QueryExecutor(db)
        query_executor.create_schema()

        loader = DataLoader(db)
        loader.load_data(args.rooms, args.students)

        queries = [
            ("Room student count", query_executor.get_room_student_count),
            ("Lowest age room", query_executor.get_lowest_avg_age_rooms),
            ("Highest age diff rooms", query_executor.get_highest_age_diff_rooms),
            ("Mixed sex rooms", query_executor.get_mixed_sex_rooms)
        ]

        for name, query_func in queries:
            try:
                result = query_func(args.format)
                output_file = os.path.join(output_dir, f'{name}.{file_ext}')
                with open(output_file, 'w', encoding='utf-8') as f:
                    if isinstance(result, bytes):
                        result = result.decode('utf-8')
                    f.write(result)
            except Exception as e:
                logger.error(f"Error writing {name} to file: {e}")
    except Exception as e:
        logger.error(f"Error: {e}")
    finally:
        db.disconnect()

if __name__ == '__main__':
    main()