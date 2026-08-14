/**
 * @file
 * @brief ESP-IDF (raw lwIP BSD socket) implementation of the bip_socket_*
 * contract used by bip.c/bip_init.c. Mirrors the Arduino WiFiUDP-based
 * bip_socket.cpp from bacnet-stack's ports/esp32, but talks directly to
 * lwIP sockets so it works over any esp_netif interface (Ethernet or WiFi)
 * without depending on the Arduino core.
 */
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include "esp_netif.h"
#include "esp_log.h"
#include "bip.h"

static const char *TAG = "bip_socket";

static int BipSockFd = -1;
static esp_netif_t *BipNetif = NULL;

/**
 * @brief Tell the socket bridge which esp_netif to report IP/netmask from.
 *        Not part of the upstream bip_socket_* contract - an addition for
 *        this ESP-IDF port, since there's no single "the network" the way
 *        Arduino's WiFi/ETH globals imply; the BACnet/IP datalink here is
 *        explicitly bound to one interface (Ethernet, toward the Delta
 *        panel), not whichever interface happens to be up.
 */
void bip_socket_esp_idf_set_netif(void *netif)
{
    BipNetif = (esp_netif_t *)netif;
}

bool bip_socket_init(uint16_t port)
{
    if (BipSockFd >= 0) {
        close(BipSockFd);
        BipSockFd = -1;
    }

    BipSockFd = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if (BipSockFd < 0) {
        ESP_LOGE(TAG, "socket() failed: errno %d", errno);
        return false;
    }

    int reuse = 1;
    setsockopt(BipSockFd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    int broadcast = 1;
    setsockopt(BipSockFd, SOL_SOCKET, SO_BROADCAST, &broadcast, sizeof(broadcast));

    int flags = fcntl(BipSockFd, F_GETFL, 0);
    fcntl(BipSockFd, F_SETFL, flags | O_NONBLOCK);

    if (BipNetif) {
        char if_name[32] = {0};
        if (esp_netif_get_netif_impl_name(BipNetif, if_name) == ESP_OK && strlen(if_name) > 0) {
            if (setsockopt(BipSockFd, SOL_SOCKET, SO_BINDTODEVICE, if_name, strlen(if_name)) == 0) {
                ESP_LOGI(TAG, "Bound BACnet socket explicitly to netif interface '%s'", if_name);
            } else {
                ESP_LOGW(TAG, "SO_BINDTODEVICE to '%s' failed: errno %d", if_name, errno);
            }
        }
    }

    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(port);

    if (bind(BipSockFd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        ESP_LOGE(TAG, "bind() to port %u failed: errno %d", port, errno);
        close(BipSockFd);
        BipSockFd = -1;
        return false;
    }

    ESP_LOGI(TAG, "UDP socket bound to port %u", port);
    return true;
}

int bip_socket_send(
    const uint8_t *dest_addr,
    uint16_t dest_port,
    const uint8_t *mtu,
    uint16_t mtu_len)
{
    if ((BipSockFd < 0) || !dest_addr || !mtu || (mtu_len == 0U)) {
        return -1;
    }

    struct sockaddr_in dest = {0};
    dest.sin_family = AF_INET;
    memcpy(&dest.sin_addr.s_addr, dest_addr, 4);
    dest.sin_port = htons(dest_port);

#ifdef CONFIG_LOG_MAXIMUM_LEVEL_DEBUG
    {
        char hex[3 * 64 + 1] = {0};
        int n = mtu_len < 64 ? mtu_len : 64;
        for (int i = 0; i < n; i++) {
            snprintf(hex + i * 3, 4, "%02x ", mtu[i]);
        }
        ESP_LOGD(
            TAG, "sendto %d.%d.%d.%d:%u len=%u: %s", dest_addr[0], dest_addr[1],
            dest_addr[2], dest_addr[3], dest_port, mtu_len, hex);
    }
#endif

    int sent = sendto(
        BipSockFd, mtu, mtu_len, 0, (struct sockaddr *)&dest, sizeof(dest));
    if (sent < 0) {
        ESP_LOGW(TAG, "sendto() failed: errno %d", errno);
        return -1;
    }
    return sent;
}

int bip_socket_receive(
    uint8_t *buf, uint16_t buf_len, uint8_t *src_addr, uint16_t *src_port)
{
    if ((BipSockFd < 0) || !buf || !src_addr || !src_port || (buf_len == 0U)) {
        return 0;
    }

    struct sockaddr_in src = {0};
    socklen_t src_len = sizeof(src);
    int received = recvfrom(
        BipSockFd, buf, buf_len, 0, (struct sockaddr *)&src, &src_len);
    if (received < 0) {
        /* EAGAIN/EWOULDBLOCK just means "nothing waiting right now" -
           expected on every poll where no packet has arrived yet. */
        return 0;
    }

    memcpy(src_addr, &src.sin_addr.s_addr, 4);
    *src_port = ntohs(src.sin_port);
    return received;
}

void bip_socket_cleanup(void)
{
    if (BipSockFd >= 0) {
        close(BipSockFd);
        BipSockFd = -1;
    }
}

bool bip_get_local_network_info(uint8_t *local_addr, uint8_t *netmask)
{
    if (!local_addr || !netmask || !BipNetif) {
        return false;
    }

    esp_netif_ip_info_t ip_info;
    if (esp_netif_get_ip_info(BipNetif, &ip_info) != ESP_OK) {
        return false;
    }

    local_addr[0] = ip_info.ip.addr & 0xFF;
    local_addr[1] = (ip_info.ip.addr >> 8) & 0xFF;
    local_addr[2] = (ip_info.ip.addr >> 16) & 0xFF;
    local_addr[3] = (ip_info.ip.addr >> 24) & 0xFF;

    netmask[0] = ip_info.netmask.addr & 0xFF;
    netmask[1] = (ip_info.netmask.addr >> 8) & 0xFF;
    netmask[2] = (ip_info.netmask.addr >> 16) & 0xFF;
    netmask[3] = (ip_info.netmask.addr >> 24) & 0xFF;

    return true;
}
