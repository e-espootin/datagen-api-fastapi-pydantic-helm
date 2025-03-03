def zz():
    from datetime import datetime
    d1 = "2025-02-23 10:04:52"
    d2 = "2025-01-12T11:51:30.788256"
    try:
        format_d1 = "%Y-%m-%d %H:%M:%S"
        format_d2 = "%Y-%m-%dT%H:%M:%S.%f"

        # Parse the strings into datetime objects
        dt1 = datetime.strptime(d1, format_d1)
        dt2 = datetime.strptime(d2, format_d2)
        print(dt1)
        print(dt2)
    except Exception as e:
        print(e)


def main():
    print("Hello from kafka-api-producer!")
    zz()


if __name__ == "__main__":
    main()
