idf_component_register(SRCS "main.c"
                        INCLUDE_DIRS "."
                        EMBED_FILES "root.html" "manage.html" "status.html" "health.html" "reset.html" "update.html" "objects.html" "wizard.html" "mqtt.html"
                        REQUIRES ethernet_init bacnet_client esp_netif esp_eth dns_server nvs_flash esp_wifi esp_http_server mqtt app_update mbedtls mdns)
