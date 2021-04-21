USE [#####]
GO
/****** Object:  StoredProcedure [dbo].[usp_LegacyDeathInsert]    Script Date: 11/30/2020 10:16:06 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/*  ---------------------------------
    | Created BY: Sean O'Donnell    |
    | Last Updated: 11/24/2020      |
    |                               |
    ---------------------------------   */

CREATE PROCEDURE [dbo].[#####]
	-- Add the parameters for the stored procedure here
	@fh BIT = NULL, -- Bool used for file type check

	-- Below variables are in both tables
	@id BIGINT, @Obit_Date SMALLDATETIME, @FName VARCHAR(MAX)=NULL, @MName VARCHAR(MAX)=NULL, @LName VARCHAR(MAX),
	@MaidenName VARCHAR(MAX)=NULL, @NickName VARCHAR(MAX)=NULL, @DOB DATETIME=NULL, @DOD DATETIME=NULL,
	@Age SMALLINT=NULL, @FuneralServiceInCity VARCHAR(MAX)=NULL, @FuneralServiceInState VARCHAR(MAX)=NULL,

	-- Below variables are only in FH
	@CurrentCity VARCHAR(MAX)=NULL, @CurrentState VARCHAR(MAX)=NULL, @AssociatedFuneralHome VARCHAR(MAX)=NULL, @Education VARCHAR(MAX)=NULL,
	@Military VARCHAR(MAX)=NULL, @Donation VARCHAR(MAX)=NULL, @FuneralServices VARCHAR(MAX)=NULL, @Salutation VARCHAR(MAX)=NULL, @Gender VARCHAR(MAX)=NULL,

	-- Below variables are only in NP
	@FuneralServiceInfo VARCHAR(MAX) = NULL, @ObituaryLink VARCHAR(MAX) = NULL, @NewspaperSource VARCHAR(MAX) = NULL, @NewspaperCity VARCHAR(MAX) = NULL,
	@NewspaperZip VARCHAR(MAX) = NULL
AS
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;
	IF NOT EXISTS (SELECT id FROM ##### WHERE id = @id)
			BEGIN
				INSERT INTO [#####]
				(
					ID,
					Obit_Date,
					Salutation,
					FName,
					MName,
					LName,
					MaidenName,
					NickName,
					DOB,
					DOD,
					Age,
					Gender,
					CurrentCity,
					CurrentState,
					FuneralServiceInCity,
					FuneralServiceInState,
					AssociatedFuneralHome,
					Education,
					Military,
					Donation,
					FuneralServices,
					ObituaryLink,
					NewspaperSource,
					NewspaperCity,
					NewspaperZip,
					hasMatch,
					SSN,
					isDead,
					src,
					Ignore,
					IgnoreReason,
					DateEntered,
					isFH,
					LastEditedByUser
				)
				VALUES
				(
					@id,
					@Obit_Date,
					@salutation,
					@FName,
					@MName,
					@LName,
					@MaidenName,
					@NickName,
					@DOB,
					@DOD,
					@Age,
					@gender,
					@CurrentCity,
					@CurrentState,
					@FuneralServiceInCity,
					@FuneralServiceInState,
					@AssociatedFuneralHome,
					@education,
					@military,
					@donation,
					@FuneralServices,
					@ObituaryLink,
					@NewspaperSource,
					@NewspaperCity,
					@NewspaperZip,
					0,
					NULL,
					0,
					NULL,
					0,
					NULL,
					GETDATE(),
					@fh,
					'LDS: Import'
				)
			END
