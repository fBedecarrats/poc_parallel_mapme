library(terra)
library(sf)
library(future)
library(tibble)
library(dplyr)
library(furrr)

# construct globally distributed polygons
(aois <- c(
  "POLYGON ((-127.4604 59.78938, -127.1124 59.79392, -127.1237 60.13207, -127.4339 60.13207, -127.4604 59.78938))",
  "POLYGON ((-91.62243 15.84818, -91.52449 15.84076, -91.52375 15.95687, -91.60944 15.95353, -91.62243 15.84818))",
  "POLYGON ((-48.37485 -13.55735, -48.61121 -13.56249, -48.60864 -13.68581, -48.362 -13.71664, -48.37485 -13.55735))",
  "POLYGON ((34.54681 1.639019, 34.87915 1.663101, 34.92249 1.889474, 34.68649 1.932822, 34.54681 1.639019))",
  "POLYGON ((120.3057 46.61764, 120.6169 46.65425, 120.5986 46.86782, 120.3789 46.84341, 120.3057 46.61764))",
  "POLYGON ((151.2656 -32.10844, 151.6211 -32.09528, 151.608 -31.8056, 151.371 -31.8056, 151.2656 -32.10844))"
) |>
    st_as_sfc(crs = st_crs(4326)) |>
    st_as_sf())
#> Simple feature collection with 6 features and 0 fields
#> Geometry type: POLYGON
#> Dimension:     XY
#> Bounding box:  xmin: -127.4604 ymin: -32.10844 xmax: 151.6211 ymax: 60.13207
#> Geodetic CRS:  WGS 84
#>                                x
#> 1 POLYGON ((-127.4604 59.7893...
#> 2 POLYGON ((-91.62243 15.8481...
#> 3 POLYGON ((-48.37485 -13.557...
#> 4 POLYGON ((34.54681 1.639019...
#> 5 POLYGON ((120.3057 46.61764...
#> 6 POLYGON ((151.2656 -32.1084...
aois$id <- 1:nrow(aois)
# print areas
units::set_units(st_area(aois), ha)
#> Units: [ha]
#> [1] 69344.81 12106.23 40886.77 92429.55 48699.86 91747.18
# get centroids
centroids <- st_centroid(aois)
#> Warning: st_centroid assumes attributes are constant over geometries
# read the COG file from openlandmap, the true file size is about ~200 GB
dem_url <- "/vsicurl/https://s3.eu-central-1.wasabisys.com/openlandmap/dtm/dtm.bareearth_ensemble_p10_30m_s_2018_go_epsg4326_v20230210.tif"
(dem <- rast(dem_url))
#> class       : SpatRaster 
#> dimensions  : 524999, 1296000, 1  (nrow, ncol, nlyr)
#> resolution  : 0.0002777778, 0.0002777778  (x, y)
#> extent      : -180, 180, -62.00056, 83.8325  (xmin, xmax, ymin, ymax)
#> coord. ref. : lon/lat WGS 84 (EPSG:4326) 
#> source      : dtm.bareearth_ensemble_p10_30m_s_2018_go_epsg4326_v20230210.tif 
#> name        : dtm.bareearth_ensemble_p10_30m_s_2018_go_epsg4326_v20230210
# plot dem and position of polygons
plot(dem, colNA = "steelblue", smooth = TRUE, range = c(-1e2, 1e4))
plot(centroids, col="red", add = TRUE, pch = 4, cex = 2)

# define example indicator functions
# mean height
dem_mean <- function(poly, dem_url){
  rast(dem_url) |>
    crop(poly) |>
    mask(poly) |>
    global("mean", na.rm = TRUE) |>
    as.numeric() -> dem_mean
  tibble(parameter = "dem",
         value = dem_mean,
         child.pid =  Sys.getpid()
  )
}
# mean tri
tri_mean <- function(poly, dem_url){
  rast(dem_url) |>
    crop(poly) |>
    mask(poly) |>
    terrain(v = "TRI") |>
    global("mean", na.rm = TRUE) |>
    as.numeric() -> tri_mean
  tibble(parameter = "tri",
         value = tri_mean,
         child.pid =  Sys.getpid()
  )
}

# define main function
main <- function(portfolio, indicators, dem_url){
  future_map_dfr(indicators,
                 function(indicator, portfolio, dem_url){
                   fun <- switch(indicator,
                                 "dem" = dem_mean,
                                 "tri" = tri_mean)
                   portfolio <- portfolio |> 
                     dplyr::group_split(id)
                   result <- future_map_dfr(portfolio,
                                            function(poly, fun, dem_url){
                                              fun(poly, dem_url)
                                            }, fun, dem_url)
                   result$parent.id <- Sys.getpid()
                   result
                 }, portfolio, dem_url, .options=furrr_options(
                   packages = c("terra", "sf")
                 )
  )
}


# execute in sequence and parallel
plan(list(tweak(multisession, workers = 2), tweak(multisession, workers = 3)))
system.time(out <- main(aois, c("dem", "tri"), dem_url))
#>    user  system elapsed 
#>   1.347   0.061  26.588
plan(sequential)

out
#> # A tibble: 12 × 4
#>    parameter   value child.pid parent.id
#>    <chr>       <dbl>     <int>     <int>
#>  1 dem        814.       14746     14630
#>  2 dem       2522.       14746     14630
#>  3 dem        574.       14748     14630
#>  4 dem       1506.       14748     14630
#>  5 dem        883.       14747     14630
#>  6 dem       1061.       14747     14630
#>  7 tri          3.05     14902     14631
#>  8 tri          7.92     14902     14631
#>  9 tri          4.62     14901     14631
#> 10 tri          4.51     14901     14631
#> 11 tri          3.96     14903     14631
#> 12 tri          6.62     14903     14631