import json
from nss_handler import status
from repository import db_get_single, db_get_all, db_delete, db_update, db_create


class DocksView:
    def get(self, handler, pk):
        url = handler.parse_url(handler.path)
        if pk != 0:
            if "_embed" in url["query_params"]:
                sql = """SELECT
                        d.id, 
                        d.location, 
                        d.capacity,
                        h.id hauler_id,
						h.name,
						h.dock_id
                        FROM Dock d
                        LEFT JOIN Hauler h
                        ON h.dock_id = d.id
                        WHERE d.id = ?
                        """
                query_results = db_get_all(sql, pk)
                dock_dict = {}
                for row in query_results:
                    individual_dock_id = row["id"]
                    if individual_dock_id not in dock_dict:
                        dock_dict[individual_dock_id] = {
                            "id": row["id"],
                            "location": row["location"],
                            "capacity": row["capacity"],
                            "haulers": [],
                        }
                    hauler = {
                        "id": row["hauler_id"],
                        "name": row["name"],
                        "dock_id": row["dock_id"],
                    }
                    if (
                        row["hauler_id"] is None
                        or row["name"] is None
                        or row["dock_id"] is None
                    ):
                        serialized_hauler = json.dumps(list(dock_dict.values()))

                    else:
                        dock_dict[individual_dock_id]["haulers"].append(hauler)

                        serialized_hauler = json.dumps(list(dock_dict.values()))
            else:
                sql = """
                SELECT
                    d.id,
                    d.location,
                    d.capacity
                FROM Dock d
                WHERE d.id = ?
                """
                query_results = db_get_single(sql, pk)
                serialized_hauler = json.dumps(dict(query_results))

            return handler.response(serialized_hauler, status.HTTP_200_SUCCESS.value)
        else:
            if "_embed" in url["query_params"]:
                sql = """SELECT
                        d.id, 
                        d.location, 
                        d.capacity,
                        h.id hauler_id,
						h.name,
						h.dock_id
                        FROM Dock d
                        LEFT JOIN Hauler h
                        ON h.dock_id = d.id
                        """
                query_results = db_get_all(sql, pk)
                dock_dict = {}
                for row in query_results:
                    individual_dock_id = row["id"]
                    if individual_dock_id not in dock_dict:
                        dock_dict[individual_dock_id] = {
                            "id": row["id"],
                            "location": row["location"],
                            "capacity": row["capacity"],
                            "haulers": [],
                        }
                    hauler = {
                        "id": row["hauler_id"],
                        "name": row["name"],
                        "dock_id": row["dock_id"],
                    }
                    if (
                        row["hauler_id"] is None
                        or row["name"] is None
                        or row["dock_id"] is None
                    ):
                        serialized_hauler = json.dumps(list(dock_dict.values()))

                    else:
                        dock_dict[individual_dock_id]["haulers"].append(hauler)

                        serialized_haulers = json.dumps(list(dock_dict.values()))
            else:
                query_results = db_get_all(
                    "SELECT d.id, d.location, d.capacity FROM Dock d", pk
                )
                haulers = [dict(row) for row in query_results]
                serialized_haulers = json.dumps(haulers)

            return handler.response(serialized_haulers, status.HTTP_200_SUCCESS.value)

    def delete(self, handler, pk):
        number_of_rows_deleted = db_delete("DELETE FROM Dock WHERE id = ?", pk)

        if number_of_rows_deleted > 0:
            return handler.response("", status.HTTP_204_SUCCESS_NO_RESPONSE_BODY.value)
        else:
            return handler.response(
                "", status.HTTP_404_CLIENT_ERROR_RESOURCE_NOT_FOUND.value
            )

    def update(self, handler, dock_data, pk):
        sql = """
        UPDATE Dock
        SET
            location = ?,
            capacity = ?
        WHERE id = ?
        """
        number_of_rows_updated = db_update(
            sql, (dock_data["location"], dock_data["capacity"], pk)
        )

        if number_of_rows_updated > 0:
            return handler.response("", status.HTTP_204_SUCCESS_NO_RESPONSE_BODY.value)
        else:
            return handler.response(
                "", status.HTTP_404_CLIENT_ERROR_RESOURCE_NOT_FOUND.value
            )

    def create(self, handler, dock_data):
        sql = """
        INSERT INTO Dock
        (location, capacity) VALUES (?,?)"""
        posted_dock = db_create(
            sql,
            (dock_data["location"], dock_data["capacity"]),
        )
        dock = {
            "id": posted_dock,
            "location": dock_data["location"],
            "capacity": dock_data["capacity"],
        }
        post_dock = json.dumps(dock)

        if posted_dock:
            return handler.response(post_dock, status.HTTP_201_SUCCESS_CREATED.value)
        else:
            return handler.response("", status.HTTP_201_SUCCESS_CREATED)
