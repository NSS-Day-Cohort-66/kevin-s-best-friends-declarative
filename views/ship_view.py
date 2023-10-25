import sqlite3
import json
from nss_handler import status
from repository import db_get_single, db_get_all, db_delete, db_update, db_create


class ShippingShipsView:
    def get(self, handler, pk):
        url = handler.parse_url(handler.path)
        if pk != 0:
            if "_expand" in url["query_params"]:
                sql = """ SELECT
                            s.id,
                            s.name,
                            s.hauler_id,
                            h.id haulerId,
                            h.name haulerName,
                            h.dock_id
                            FROM Ship s
                            JOIN Hauler h
                            ON h.id = s.hauler_id
                            WHERE s.id = ?"""
                query_results = db_get_single(sql, pk)
                query_results_dict = dict(query_results)
                hauler = {
                    "id": query_results_dict["haulerId"],
                    "name": query_results_dict["haulerName"],
                    "dock_id": query_results_dict["dock_id"],
                }
                ship = {
                    "id": query_results_dict["id"],
                    "name": query_results_dict["name"],
                    "hauler_id": query_results_dict["hauler_id"],
                    "hauler": hauler,
                }
                serialized_hauler = json.dumps(dict(ship))
            else:
                sql = "SELECT s.id, s.name, s.hauler_id FROM Ship s WHERE s.id = ?"
                query_results = db_get_single(sql, pk)
                serialized_hauler = json.dumps(dict(query_results))

            return handler.response(serialized_hauler, status.HTTP_200_SUCCESS.value)
        else:
            if "_expand" in url["query_params"]:
                sql = """ SELECT
                            s.id,
                            s.name,
                            s.hauler_id,
                            h.id haulerId,
                            h.name haulerName,
                            h.dock_id
                            FROM Ship s
                            JOIN Hauler h
                            ON h.id = s.hauler_id"""
                query_results = db_get_all(sql, pk)
                ships = []
                for row in query_results:
                    hauler = {
                        "id": row["haulerId"],
                        "name": row["haulerName"],
                        "dock_id": row["dock_id"],
                    }
                    ship = {
                        "id": row["id"],
                        "name": row["name"],
                        "hauler_id": row["hauler_id"],
                        "hauler": hauler,
                    }
                    ships.append(ship)
                    serialized_haulers = json.dumps(ships)
            else:
                sql = "SELECT s.id, s.name, s.hauler_id FROM Ship s"
                query_results = db_get_all(sql, pk)
                haulers = [dict(row) for row in query_results]
                serialized_haulers = json.dumps(haulers)

            return handler.response(serialized_haulers, status.HTTP_200_SUCCESS.value)

    def delete(self, handler, pk):
        number_of_rows_deleted = db_delete("DELETE FROM Ship WHERE id = ?", pk)

        if number_of_rows_deleted > 0:
            return handler.response("", status.HTTP_204_SUCCESS_NO_RESPONSE_BODY.value)
        else:
            return handler.response(
                "", status.HTTP_404_CLIENT_ERROR_RESOURCE_NOT_FOUND.value
            )

    def update(self, handler, ship_data, pk):
        sql = """
        UPDATE Ship
        SET
            name = ?,
            hauler_id = ?
        WHERE id = ?
        """
        number_of_rows_updated = db_update(
            sql, (ship_data["name"], ship_data["hauler_id"], pk)
        )

        if number_of_rows_updated > 0:
            return handler.response("", status.HTTP_204_SUCCESS_NO_RESPONSE_BODY.value)
        else:
            return handler.response(
                "", status.HTTP_404_CLIENT_ERROR_RESOURCE_NOT_FOUND.value
            )

    def create(self, handler, ship_data):
        sql = """
        INSERT INTO Ship
        (name, hauler_id) VALUES (?,?)"""
        posted_ship = db_create(
            sql,
            (ship_data["name"], ship_data["hauler_id"]),
        )
        ship = {
            "id": posted_ship,
            "name": ship_data["name"],
            "hauler_id": ship_data["hauler_id"],
        }
        post_ship = json.dumps(ship)

        if posted_ship:
            return handler.response(post_ship, status.HTTP_201_SUCCESS_CREATED.value)
        else:
            return handler.response("", status.HTTP_201_SUCCESS_CREATED)
