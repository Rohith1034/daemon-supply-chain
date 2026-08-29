from datetime import datetime, timezone



def log_event_success(
        event_name,
        details: dict
):
    """
    Standard success event log.
    """


    print("\n" + "=" * 60)

    print(
        f"EVENT : {event_name}"
    )


    for key, value in details.items():

        formatted_key = (
            key.replace("_", " ")
               .upper()
        )

        print(
            f"{formatted_key:<20}: {value}"
        )


    print(
        f"TIME : {datetime.now(timezone.utc)}"
    )


    print(
        "STATUS : SUCCESS"
    )

    print("=" * 60 + "\n")



def log_event_failure(
        event_name,
        error
):
    """
    Standard failure log.
    """


    print("\n" + "=" * 60)

    print(
        f"EVENT : {event_name}"
    )


    print(
        f"ERROR : {error}"
    )


    print(
        f"TIME : {datetime.now(timezone.utc)}"
    )


    print(
        "STATUS : FAILED"
    )


    print("=" * 60 + "\n")