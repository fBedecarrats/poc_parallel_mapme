library(terra)
library(sf)
library(future)
library(furrr)
library(tidyverse)

# construct globally distributed polygons
aois_6 <- c(
  "POLYGON ((-127.4604 59.78938, -127.1124 59.79392, -127.1237 60.13207, -127.4339 60.13207, -127.4604 59.78938))",
  "POLYGON ((-91.62243 15.84818, -91.52449 15.84076, -91.52375 15.95687, -91.60944 15.95353, -91.62243 15.84818))",
  "POLYGON ((-48.37485 -13.55735, -48.61121 -13.56249, -48.60864 -13.68581, -48.362 -13.71664, -48.37485 -13.55735))",
  "POLYGON ((34.54681 1.639019, 34.87915 1.663101, 34.92249 1.889474, 34.68649 1.932822, 34.54681 1.639019))",
  "POLYGON ((120.3057 46.61764, 120.6169 46.65425, 120.5986 46.86782, 120.3789 46.84341, 120.3057 46.61764))",
  "POLYGON ((151.2656 -32.10844, 151.6211 -32.09528, 151.608 -31.8056, 151.371 -31.8056, 151.2656 -32.10844))") |>
    st_as_sfc(crs = st_crs(4326)) |>
    st_as_sf()

aois_6$id <- 1:nrow(aois_6)

# print areas
units::set_units(st_area(aois_6), ha)

# get centroids
centroids <- st_centroid(aois_6)

# read the COG file from openlandmap, the true file size is about ~200 GB
dem_url <- "/vsicurl/https://s3.eu-central-1.wasabisys.com/openlandmap/dtm/dtm.bareearth_ensemble_p10_30m_s_2018_go_epsg4326_v20230210.tif"
(dem <- rast(dem_url))

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

# Create larger datasets
aois_60 <- bind_rows(replicate(10, aois_6, simplify = FALSE))
aois_60$id <- 1:nrow(aois_60)

# With 600 assets
aois_600 <- bind_rows(replicate(100, aois_6, simplify = FALSE))
aois_600$id <- 1:nrow(aois_600)

plans <- tibble(workers = list(2,
                               4, c(2,2),
                               8, c(2, 4), c(4,2),
                               16, c(4, 4), c(2, 8), c(8,2),
                               24, c(2, 12), c(3, 8), c(4,6))) %>%
  mutate(levels_parallel = lengths(workers),
         plan = case_when(
           levels_parallel == 2 ~ paste0("plan(list(tweak(cluster, workers = ", 
                                         workers[1],
                                         "), tweak(cluster, workers = ",
                                         workers[2],")))"),
           levels_parallel == 1 ~ paste0("plan(cluster, workers = ", workers,")"))) %>% 
  bind_rows(mutate(., plan = str_replace(plan, "cluster", "multicore")),
            mutate(., plan = str_replace(plan, "cluster", "multisession"))) %>%
  add_row(workers = list(1), levels_parallel = 1, plan = "plan(sequential)")

# name of aoi portfolios
aois_n <- c("aois_6", "aois_60", "aois_600")

# group aois in 1 stack
aois_stack <- list(aois_6, aois_60, aois_600)

# Using for loops on purpose to avoid confusion with vectorized execution
for (i in 1:length(plans$plan)) {
  for (j in 1:3) {
    eval(parse(text = plans$plan[i]))
    print(paste("evaluate", aois_n[j], "with", plans$plan[i]))
 #   my_poly <- eval(parse(polys[j]))
    elapsed <- system.time(main(aois_stack[[j]], 
                                c("dem", "tri"), dem_url))[["elapsed"]]
    plans[i, aois_n[j]] <- elapsed 
  }
}

write_csv(plans, "benchmark_outputs.csv")
