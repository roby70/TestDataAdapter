USE [Test]
GO

/****** Object:  Table [dbo].[MyTable]    Script Date: 21/11/2023 12:43:43 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[MyTable](
	[SurrogateId] [int] IDENTITY(1,1) NOT NULL,
	[MainId] [int] NULL,
	[DetailId] [varchar](50) NULL,
	[Description] [varchar](255) NULL,
PRIMARY KEY CLUSTERED 
(
	[SurrogateId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO


/****** Object:  Index [UX_MyTable]    Script Date: 21/11/2023 12:44:01 ******/
CREATE UNIQUE NONCLUSTERED INDEX [UX_MyTable] ON [dbo].[MyTable]
(
	[MainId] ASC,
	[DetailId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO


