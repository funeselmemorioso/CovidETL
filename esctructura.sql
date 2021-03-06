USE [master]
GO
/****** Object:  Database [coronavirus]    Script Date: 28/11/2020 14:01:43 ******/
CREATE DATABASE [coronavirus]
 CONTAINMENT = NONE
 ON  PRIMARY 
( NAME = N'coronavirus', FILENAME = N'D:\SqlServerBBDD11\coronavirus.mdf' , SIZE = 8192KB , MAXSIZE = UNLIMITED, FILEGROWTH = 65536KB )
 LOG ON 
( NAME = N'coronavirus_log', FILENAME = N'D:\SqlServerBBDD11\coronavirus_log.ldf' , SIZE = 73728KB , MAXSIZE = 2048GB , FILEGROWTH = 65536KB )
GO
ALTER DATABASE [coronavirus] SET COMPATIBILITY_LEVEL = 140
GO
IF (1 = FULLTEXTSERVICEPROPERTY('IsFullTextInstalled'))
begin
EXEC [coronavirus].[dbo].[sp_fulltext_database] @action = 'enable'
end
GO
ALTER DATABASE [coronavirus] SET ANSI_NULL_DEFAULT OFF 
GO
ALTER DATABASE [coronavirus] SET ANSI_NULLS OFF 
GO
ALTER DATABASE [coronavirus] SET ANSI_PADDING OFF 
GO
ALTER DATABASE [coronavirus] SET ANSI_WARNINGS OFF 
GO
ALTER DATABASE [coronavirus] SET ARITHABORT OFF 
GO
ALTER DATABASE [coronavirus] SET AUTO_CLOSE ON 
GO
ALTER DATABASE [coronavirus] SET AUTO_SHRINK OFF 
GO
ALTER DATABASE [coronavirus] SET AUTO_UPDATE_STATISTICS ON 
GO
ALTER DATABASE [coronavirus] SET CURSOR_CLOSE_ON_COMMIT OFF 
GO
ALTER DATABASE [coronavirus] SET CURSOR_DEFAULT  GLOBAL 
GO
ALTER DATABASE [coronavirus] SET CONCAT_NULL_YIELDS_NULL OFF 
GO
ALTER DATABASE [coronavirus] SET NUMERIC_ROUNDABORT OFF 
GO
ALTER DATABASE [coronavirus] SET QUOTED_IDENTIFIER OFF 
GO
ALTER DATABASE [coronavirus] SET RECURSIVE_TRIGGERS OFF 
GO
ALTER DATABASE [coronavirus] SET  DISABLE_BROKER 
GO
ALTER DATABASE [coronavirus] SET AUTO_UPDATE_STATISTICS_ASYNC OFF 
GO
ALTER DATABASE [coronavirus] SET DATE_CORRELATION_OPTIMIZATION OFF 
GO
ALTER DATABASE [coronavirus] SET TRUSTWORTHY OFF 
GO
ALTER DATABASE [coronavirus] SET ALLOW_SNAPSHOT_ISOLATION OFF 
GO
ALTER DATABASE [coronavirus] SET PARAMETERIZATION SIMPLE 
GO
ALTER DATABASE [coronavirus] SET READ_COMMITTED_SNAPSHOT OFF 
GO
ALTER DATABASE [coronavirus] SET HONOR_BROKER_PRIORITY OFF 
GO
ALTER DATABASE [coronavirus] SET RECOVERY SIMPLE 
GO
ALTER DATABASE [coronavirus] SET  MULTI_USER 
GO
ALTER DATABASE [coronavirus] SET PAGE_VERIFY CHECKSUM  
GO
ALTER DATABASE [coronavirus] SET DB_CHAINING OFF 
GO
ALTER DATABASE [coronavirus] SET FILESTREAM( NON_TRANSACTED_ACCESS = OFF ) 
GO
ALTER DATABASE [coronavirus] SET TARGET_RECOVERY_TIME = 60 SECONDS 
GO
ALTER DATABASE [coronavirus] SET DELAYED_DURABILITY = DISABLED 
GO
ALTER DATABASE [coronavirus] SET QUERY_STORE = OFF
GO
USE [coronavirus]
GO
ALTER DATABASE SCOPED CONFIGURATION SET IDENTITY_CACHE = ON;
GO
ALTER DATABASE SCOPED CONFIGURATION SET LEGACY_CARDINALITY_ESTIMATION = OFF;
GO
ALTER DATABASE SCOPED CONFIGURATION FOR SECONDARY SET LEGACY_CARDINALITY_ESTIMATION = PRIMARY;
GO
ALTER DATABASE SCOPED CONFIGURATION SET MAXDOP = 0;
GO
ALTER DATABASE SCOPED CONFIGURATION FOR SECONDARY SET MAXDOP = PRIMARY;
GO
ALTER DATABASE SCOPED CONFIGURATION SET PARAMETER_SNIFFING = ON;
GO
ALTER DATABASE SCOPED CONFIGURATION FOR SECONDARY SET PARAMETER_SNIFFING = PRIMARY;
GO
ALTER DATABASE SCOPED CONFIGURATION SET QUERY_OPTIMIZER_HOTFIXES = OFF;
GO
ALTER DATABASE SCOPED CONFIGURATION FOR SECONDARY SET QUERY_OPTIMIZER_HOTFIXES = PRIMARY;
GO
USE [coronavirus]
GO
/****** Object:  Table [dbo].[datos_generales]    Script Date: 28/11/2020 14:01:44 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[datos_generales](
	[id] [int] IDENTITY(1,1) NOT NULL,
	[pais] [varchar](250) NOT NULL,
	[fecha] [datetime] NOT NULL,
	[confirmados] [int] NULL,
	[muertos] [int] NULL,
	[recuperados] [int] NULL,
 CONSTRAINT [PK_datos_generales] PRIMARY KEY CLUSTERED 
(
	[pais] ASC,
	[fecha] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[equivalencias_codigos]    Script Date: 28/11/2020 14:01:45 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[equivalencias_codigos](
	[cod_universal] [nvarchar](50) NULL,
	[descripcion_pais_datos_generales] [nvarchar](250) NULL,
	[descripcion_pais] [nvarchar](250) NULL
) ON [PRIMARY]
GO
/****** Object:  StoredProcedure [dbo].[varias_consultas]    Script Date: 28/11/2020 14:01:45 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE procedure [dbo].[varias_consultas]
as
select  * from  [datos_generales] where pais = 'argentina'
select  * from  [datos_generales] where pais = 'us'
select  * from  [datos_generales] where pais = 'brazil'
select  * from  [datos_generales] where pais = 'bolivia'
select  * from  [datos_generales] where pais = 'russia'

-- Ultimo dato por paìs
select * from [datos_generales] where fecha = (select max(fecha) from [datos_generales]) 
order by confirmados desc, muertos desc, recuperados desc

-- Total mundial confirmados
select sum(confirmados) as 'total' from [datos_generales] where fecha = (select max(fecha) from [datos_generales])

-- total mundial muertos
select sum(muertos) as 'total' from [datos_generales] where fecha = (select max(fecha) from [datos_generales])

-- total mundial curados
select sum(recuperados) as 'total' from [datos_generales] where fecha = (select max(fecha) from [datos_generales])

-- truncate table [datos_generales]

select pm.[2018] as 'Población Total', 
dg.pais, 
CAST(fecha as date) as 'fecha', 
confirmados, 
muertos, 
recuperados,
confirmados-muertos-recuperados as 'activos', 
cast(cast(recuperados as decimal)/CAST(confirmados as decimal)*100 as numeric(36,2)) as '% Recup./Contag.',
--cast(muertos as decimal)/CAST(confirmados as decimal)*100 as '% Mortalidad',
cast(cast(muertos as decimal)/CAST(confirmados as decimal)*100 as numeric(36,2)) as '% Mortalidad',
CASE WHEN pm.[2018] IS NOT NULL AND pm.[2018]<>'' THEN 
--cast(muertos as decimal)/CAST(pm.[2018] as decimal)*100000
cast(cast(muertos as decimal)/CAST(pm.[2018] as decimal)*100000 as numeric(36,2))
ELSE NULL
END as 'Muertos c/100.000 Habitantes',
CASE WHEN pm.[2018] IS NOT NULL AND pm.[2018]<>'' THEN 
--round(cast(confirmados as decimal)/CAST(pm.[2018] as decimal)*100000, 2)
cast(cast(confirmados as decimal)/CAST(pm.[2018] as decimal)*100000 as numeric(36,2))
ELSE NULL
END as 'Contagiados c/100.000 Habitantes'   
FROM [datos_generales] dg
left join equivalencias_codigos ec 
on dg.pais = ec.descripcion_pais_datos_generales 
left join poblacion.dbo.v_ultimo_dato_poblacion_mundial pm
on ec.descripcion_pais = pm.pais 
where fecha = (select max(fecha) from [datos_generales]) 
order by confirmados desc
--order by 'Contagiados c/100.000 Habitantes'  
--select * from poblacion.dbo.v_ultimo_dato_poblacion_mundial






select pais,
CAST(fecha as date) as 'fecha', 
confirmados, 
muertos, 
recuperados,
confirmados-muertos-recuperados as 'activos'
FROM [datos_generales] where pais = 'Argentina'





GO
USE [master]
GO
ALTER DATABASE [coronavirus] SET  READ_WRITE 
GO
