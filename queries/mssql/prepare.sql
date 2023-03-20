




-- Tuning for Q13
CREATE FULLTEXT CATALOG [fts] WITH ACCENT_SENSITIVITY = ON AS DEFAULT;


CREATE FULLTEXT INDEX ON [dbo].[orders](
[o_comment] LANGUAGE 'English')
KEY INDEX [orders_pk]ON ([fts], FILEGROUP [PRIMARY])
WITH (CHANGE_TRACKING = AUTO, STOPLIST = SYSTEM);
