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


plan(sequential)
system.time(out <- main(aois, c("dem", "tri"), dem_url))
# user  system elapsed 
# 2.547   0.650  19.507 
plan(list(tweak(cluster, workers = 4), tweak(cluster, workers = 4)))
system.time(out <- main(aois, c("dem", "tri"), dem_url))
# user  system elapsed 
# 2.559   0.332  36.415 


# Now let's try with more assets
many_aois <- bind_rows(replicate(10, aois, simplify = FALSE))
many_aois$id <- 1:nrow(many_aois)

plan(sequential)
system.time(out <- main(many_aois, c("dem", "tri"), dem_url))
# user  system elapsed 
# 21.570   5.551 191.902 

plan(list(tweak(cluster, workers = 4), tweak(cluster, workers = 4)))
system.time(out <- main(many_aois, c("dem", "tri"), dem_url))
# user  system elapsed 
# 4.689   0.360  57.267 

## TWEAK PLAN ---------------------------------

plan(sequential)
plan(list(tweak(cluster, workers = 2), tweak(cluster, workers = 4)))
system.time(out <- main(many_aois, c("dem", "tri"), dem_url))
# user  system elapsed 
# 5.589   0.565  67.473 

plan(sequential)
plan(list(tweak(cluster, workers = 4), tweak(cluster, workers = 2)))
system.time(out <- main(many_aois, c("dem", "tri"), dem_url))
# user  system elapsed 
# 5.983   0.600  73.072 

plan(sequential)
plan(cluster, workers = 4)
system.time(out <- main(many_aois, c("dem", "tri"), dem_url))
# user  system elapsed 
# 9.946   0.926 109.158 

plan(sequential)
plan(cluster, workers = 8)
system.time(out <- main(many_aois, c("dem", "tri"), dem_url))
# user  system elapsed 
# 6.725   0.770  74.830 

plan(sequential)
plan(list(tweak(cluster, workers = 2), tweak(cluster, workers = 8)))
system.time(out <- main(many_aois, c("dem", "tri"), dem_url))
# user  system elapsed 
# 6.459   0.846  70.975

plan(sequential)
plan(list(tweak(multisession, workers = 4), tweak(multisession, workers = 4)))
system.time(out <- main(many_aois, c("dem", "tri"), dem_url))
# user  system elapsed 
# 5.064   0.410  61.576 

## END TWEAK PLAN --------------------------------

# Now let's try with more assets
very_many_aois <- bind_rows(replicate(100, aois, simplify = FALSE))
very_many_aois$id <- 1:nrow(very_many_aois)

plan(sequential)
plan(list(tweak(cluster, workers = 4), tweak(cluster, workers = 4)))
system.time(out <- main(very_many_aois, c("dem", "tri"), dem_url))
# user  system elapsed 
# 29.505   2.516 311.656



plan(sequential)
plan(list(tweak(cluster, workers = 4), tweak(cluster, workers = 4)))
system.time(out <- main2(many_aois, c("dem", "tri"), dem_url))