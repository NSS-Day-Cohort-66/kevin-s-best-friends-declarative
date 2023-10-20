import json
from nss_handler import status
from repository import db_get_single, db_get_all, db_delete, db_update, db_create


class HaulerView:
    def get(self, handler, pk, query_params):
        if pk != 0:
            if "_expand" in query_params:
                sql = """
                        SELECT
                        h.id, 
                        h.name, 
                        h.dock_id,
                        d.id,
                        d.location,
                        d.capacity
                        FROM Hauler h
                        JOIN Dock d
                        ON d.id = h.dock_id
                        WHERE h.id = ?"""
                query_results = db_get_single(sql, pk)
                query_results_dict = dict(query_results)
                dock = {
                    "id": query_results_dict["dock_id"],
                    "location": query_results_dict["location"],
                    "capacity": query_results_dict["capacity"],
                }
                hauler = {
                    "id": query_results_dict["id"],
                    "name": query_results_dict["name"],
                    "dock_id": query_results_dict["dock_id"],
                    "dock": dock,
                }
                dictionary_version_of_object = dict(hauler)
                serialized_hauler = json.dumps(dictionary_version_of_object)
            else:
                sql = "SELECT h.id, h.name, h.dock_id FROM Hauler h WHERE h.id = ?"
                query_results = db_get_single(sql, pk)
                serialized_hauler = json.dumps(dict(query_results))

            return handler.response(serialized_hauler, status.HTTP_200_SUCCESS.value)
        else:
            if "_expand" in query_params:
                sql = """
                        SELECT
                        h.id, 
                        h.name, 
                        h.dock_id,
                        d.id dockId,
                        d.location,
                        d.capacity
                        FROM Hauler h
                        JOIN Dock d
                        ON d.id = h.dock_id"""
                query_results = db_get_all(sql)
                haulers = []
                for row in query_results:
                    dock = {
                        "id": row["dockId"],
                        "location": row["location"],
                        "capacity": row["capacity"],
                    }
                    hauler = {
                        "id": row["id"],
                        "name": row["name"],
                        "dock_id": row["dock_id"],
                        "dock": dock,
                    }
                    haulers.append(hauler)

            else:
                sql = "SELECT h.id, h.name, h.dock_id FROM Hauler h"
                query_results = db_get_all(sql)
                haulers = [dict(row) for row in query_results]
            serialized_haulers = json.dumps(haulers)

            return handler.response(serialized_haulers, status.HTTP_200_SUCCESS.value)

    def delete(self, handler, pk):
        number_of_rows_deleted = db_delete("DELETE FROM Hauler WHERE id = ?", pk)

        if number_of_rows_deleted > 0:
            return handler.response("", status.HTTP_204_SUCCESS_NO_RESPONSE_BODY.value)
        else:
            return handler.response(
                "", status.HTTP_404_CLIENT_ERROR_RESOURCE_NOT_FOUND.value
            )

    def update(self, handler, hauler_data, pk):
        sql = """
        UPDATE Hauler
        SET
            name = ?,
            dock_id = ?
        WHERE id = ?
        """
        number_of_rows_updated = db_update(
            sql, (hauler_data["name"], hauler_data["dock_id"], pk)
        )

        if number_of_rows_updated > 0:
            return handler.response("", status.HTTP_204_SUCCESS_NO_RESPONSE_BODY.value)
        else:
            return handler.response(
                "", status.HTTP_404_CLIENT_ERROR_RESOURCE_NOT_FOUND.value
            )

    def create(self, handler, hauler_data):
        sql = """
        INSERT INTO Hauler
        (name, dock_id) VALUES (?,?)"""
        posted_hauler = db_create(
            sql,
            (hauler_data["name"], hauler_data["dock_id"]),
        )

        if posted_hauler:
            return handler.response("", status.HTTP_201_SUCCESS_CREATED.value)
        else:
            return handler.response("", status.HTTP_201_SUCCESS_CREATED)
