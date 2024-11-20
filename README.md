# Carver 

SDK to help build ecosystem intelligence by acquiring, analyzing, and
applying content from variety of sources including YouTube, Reddit etc
This is designed to be used as a 'tool' with LLMs.

<img src="images/carver-github.png" alt="Carver GitHub" width="150"/>

## Usecase

Most products - opensource or commercial - die not because of lack of
smart people but it doesnt fit the customer's ever evolving
needs. With AI, this will become 10x harder because customer/user
preferences, new products, and new competitors/options are emering at
10x speed. There is a need to understand and engage customers/users
_now_ - not 3 or 6 months from now. There is no single source or
recipe for this. This SDK will help streamline the feed processing
with the specific intent of building this intelligence and for
integration with LLMs both on the input side and output side.

## Installation

1. Clone the repository:
```bash
git clone https://github.com/pingali/carver.git
cd carver
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Create a configuration file at `~/.carver/client.ini` with your Supabase credentials:
```ini
SUPABASE_URL=your_supabase_url
SUPABASE_KEY=your_supabase_key
```

## Usage

Support for autocomplete
```bash
eval "$(_CARVER_RUN_COMPLETE=bash_source carver-run)"
```

### Entity Management

```bash
# Search by name
carver-run entity list --search "test"

# List entities created in the last week
carver-run entity list --created-since 1w

# List entities updated in the last day with grid format
carver-run entity list --updated-since 1d --format grid

# Combined search
carver-run entity list --search "test" --entity-type ORGANIZATION --active

# Show detailed entity info
carver-run entity show 1

```

### Source Management

```bash

# Traditional playlist URL
carver-run source add --entity-id 1 --url "https://www.youtube.com/playlist?list=PLj6h78yzYM2Pw4mRw4S-1p_xLARMqPkA7"

# Video URL with playlist
carver-run source add --entity-id 1 --url "https://www.youtube.com/watch?v=WI7wp8xwgkk&list=PLj6h78yzYM2Pw4mRw4S-1p_xLARMqPkA7"

# Add a YouTube channel
carver-run source add --entity-id 1 --url "https://www.youtube.com/channel/UCCezIgC97PvUuR4_gbFUs5g"

# Add a regular RSS feed
carver-run source add --entity-id 1 --url "https://hnrss.org/newest"

# Add a tech blog feed
carver-run source add --entity-id 1 --url "https://blog.golang.org/feed.atom"

# Add a news feed
carver-run source add --entity-id 1 --url "http://rss.cnn.com/rss/edition.rss"

# Add a GitHub repository
carver-run source add --entity-id 1 --url "https://github.com/openai/whisper"

# Add a subreddit
carver-run source add --entity-id 1 --url "https://www.reddit.com/r/Python/"

# Add an RSS feed
carver-run source add --entity-id 1 --url "https://news.ycombinator.com/rss"

# Add with custom name and config
carver-run source add --entity-id 1 --url "https://www.youtube.com/channel/UC8butISFwT-Wl7EV0hUK0BQ" \
    --name "FreeCodeCamp" --config '{"language": "en", "categories": ["programming", "education"]}'

# Add a new source
carver-run source add --name "Python Blog" --entity-id 1 --platform RSS \
    --source-type FEED --source-identifier "python-blog" --url "https://blog.python.org/feed/"

# List sources with filters
carver-run source list --platform GITHUB --updated-since 1w

# Show detailed source info
carver-run source show 1

# Update source metadata
carver-run source update 1 --metadata '{"last_error": null, "items_processed": 150}'

```

```bash
# Sync items from a source
carver-run item sync --source-id 1

# Sync last 100 videos from a channel
python cli.py item sync --source-id 1 --max-results 100

# Sync entire playlist (no limit)
python cli.py item sync --source-id 2

# Sync specific fields from last 500 videos
python cli.py item sync --source-id 1 --max-results 500 --fields "title,description,published_at"

# Sync specific fields only
carver-run item sync --source-id 1 --fields "title,content,published_at"

# Activate specific items
carver-run item activate --source-id 1 --identifiers "video1,video2,video3"

# Search items with filters
carver-run item search --source-id 1 --active --published-since 1w --title-search "python"

# Show detailed item information
carver-run item show 1

# Search with pagination
carver-run item search --limit 50 --offset 100 --format grid
```

## Error Handling

The CLI includes comprehensive error handling for:
- Invalid JSON in config/metadata fields
- Database connection issues
- Invalid entity types
- Missing required fields

## Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature-name`
3. Commit your changes: `git commit -am 'Add feature'`
4. Push to the branch: `git push origin feature-name`
5. Submit a pull request

## License

[Your chosen license]

## Support

For issues and feature requests, please create an issue in the GitHub repository.

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## Requirements

- Python 3.7+
- pytube>=15.0.0
- feedparser>=6.0.10
- click>=8.1.7
- rich>=13.3.5
- python-dotenv>=1.0.0

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Sponsor

Carver Agents, Inc.

Others are welcome! We see broad application of this SDK beyond what Carver Agents intends to build.

## Note

This tool is meant for personal use and should be used in accordance with YouTube's terms of service and content policies.
