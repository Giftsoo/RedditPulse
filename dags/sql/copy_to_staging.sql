TRUNCATE TABLE reddit_posts_staging;

COPY reddit_posts_staging
FROM 's3://reddit-data-engineering-giftson-us-east-1/raw/reddit/incoming/'
IAM_ROLE DEFAULT
FORMAT AS CSV
DELIMITER ','
IGNOREHEADER 1
QUOTE '"'
ACCEPTINVCHARS
TRUNCATECOLUMNS
BLANKSASNULL
EMPTYASNULL
MAXERROR 1000
REGION 'us-east-1';