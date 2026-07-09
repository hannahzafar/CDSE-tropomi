import argparse
from eodag import EODataAccessGateway, SearchResult
from eodag import setup_logging
import os


def main():
    # parse inputs
    parser = argparse.ArgumentParser(
        description="Search Copernicus Data Space Ecosystem via EODAG."
    )

    parser.add_argument(
        "-c",
        "--collection",
        type=str,
        default="S5P_L2_AER_AI",
        help="EODAG product type (e.g., S5P_L2_AER_AI, S5P_L2_AER_LH). Default: S5P_L2_AER_AI",
    )
    parser.add_argument(
        "-s",
        "--start",
        type=str,
        required=True,
        help="Start date in YYYY-MM-DD format (e.g., 2023-08-01)",
    )
    parser.add_argument(
        "-e",
        "--end",
        type=str,
        required=True,
        help="End date in YYYY-MM-DD format (e.g., 2023-08-02)",
    )

    args = parser.parse_args()
    # Set download space
    workspace = "eodag_workspace_download"
    if not os.path.isdir(workspace):
        os.mkdir(workspace)
    os.environ["EODAG__COP_DATASPACE__DOWNLOAD__OUTPUT_DIR"] = os.path.abspath(
        workspace
    )

    # Open access gateway and set Copernicus Data Space Ecosystem as provider
    dag = EODataAccessGateway()
    dag.set_preferred_provider("cop_dataspace")

    # Search for Sentinel-5P Aerosol Index data over a specific time
    search_results = dag.search_all(
        collection=args.collection,
        start=args.start,
        end=args.end,
        raise_errors=True,
    )
    total_count = len(search_results)
    print(f"Found {total_count} products.")

    # Check product is available for download/filter search
    online_search_results = search_results.filter_property(
        **{"order:status": "succeeded"}
    )
    search_to_download = SearchResult(online_search_results)
    print(f"Total files available for download: {len(search_to_download)}")

    # Check sizes
    total_size = 0
    total_NA = 0
    for product in search_to_download:
        # print(f"{product}, properties: {product.properties.keys()}") # Check availalbe properties
        size_bytes = product.properties.get("file:size")
        if size_bytes:
            size_in_mb = int(size_bytes) / (1024 * 1024)
            total_size = total_size + size_in_mb
            # print(f"{product}, Est size: {size_in_mb:.2f} MB")
        else:
            total_NA = total_NA + 1
            # print(f"{product}, Est size: not avail")
    print(
        f"Total size of download: {total_size:.2f} MB \nNumber of products without size property: {total_NA}\n\n"
    )

    from concurrent.futures import ThreadPoolExecutor

    setup_logging(verbose=2, no_progress_bar=True)
    products_to_download = search_to_download
    count_download = 0
    count_fail = 0
    for product in products_to_download:
        try:
            download_path = dag.download(product)
            print(f"Download complete: {download_path}\n")
            count_download += 1
        except Exception as e:
            title = product.properties.get("title", "Unknown Product")
            print(f"Failed to download {title}. \nError: {e}\n")
            count_fail += 1

    print(f"\n\n{count_download}/{len(products_to_download)} Downloads complete")


if __name__ == "__main__":
    main()
