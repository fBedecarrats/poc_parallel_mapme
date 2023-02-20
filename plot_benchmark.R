library(tidyverse)
library(readxl)
bench <- read_excel("Benchmarks_mapme.xlsx",
                    col_types = c("text", "text", "numeric", 
                                  "numeric", "numeric")) %>%
  mutate(workers = paste("Workers =", workers),
         workers = factor(workers, levels = c( "Workers = 1", "Workers = 4", 
                                               "Workers = 8", "Workers = 16",
                                               "Workers = 24"))) %>%
  pivot_longer(cols = ends_with("polygons"), names_to = "dataset_size",
               values_to = "elapsed")


bench %>%
  ggplot(aes(x = config, y = elapsed)) +
  geom_col() + 
  facet_grid(dataset_size ~ workers, scales = "free") + 
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) + 
  labs(x = "Paralellization setting",
       y = "Processing time (in seconds)")
  
