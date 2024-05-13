import argparse
from concurrent import futures
from typing import Iterable, Optional

import grpc
import threading

import tic_tac_toe_pb2_grpc as ttt_grpc
import tic_tac_toe_pb2 as ttt


def get_winner(moves: Iterable[ttt.Move]) -> Optional[ttt.Mark]:
    winning_combinations = (
        (1, 2, 3), (4, 5, 6), (7, 8, 9),  # Rows
        (1, 4, 7), (2, 5, 8), (3, 6, 9),  # Cols
        (1, 5, 9), (3, 5, 7),  # Diagonals
    )

    x_moves = []
    o_moves = []

    for move in moves:
        if move.mark == ttt.MARK_CROSS:
            x_moves.append(move.cell)
        elif move.mark == ttt.MARK_NOUGHT:
            o_moves.append(move.cell)

    for combination in winning_combinations:
        if all((cell in x_moves) for cell in combination):
            return ttt.MARK_CROSS
        if all((cell in o_moves) for cell in combination):
            return ttt.MARK_NOUGHT

    return None


class TicTacToeServicer(ttt_grpc.TicTacToeServicer):
    def __init__(self):
        self.games = {}
        self.lock = threading.Lock()

    def CreateGame(self, request, context):
        with self.lock:
            print("CreateGame()")
            game_id = len(self.games) + 1
            game = ttt.Game(id=game_id, is_finished=False, turn=ttt.MARK_CROSS)
            self.games[game_id] = game
            return game

    def GetGame(self, request, context):
        game_id = request.game_id
        with self.lock:
            print(f"GetGame(game_id={game_id})")
            if game_id not in self.games:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details('Game not found')
                return ttt.Game()
            return self.games[game_id]

    def MakeMove(self, request, context):
        game_id = request.game_id
        move = request.move
        with self.lock:
            print(f"MakeMove(game_id={game_id}, move=Move(mark={move.mark}, cell={move.cell})")

            if game_id not in self.games:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details('Game not found')
                return ttt.Game()

            game = self.games[game_id]
            if game.is_finished:
                context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
                context.set_details('Game is already finished')
                return game

            if 1 > move.cell > 9:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details('Invalid move [Move must be in range [1,9]]')
                return game

            if game.turn != move.mark:
                context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
                context.set_details("It's not player's turn")
                return game

            occupied = set(move.cell for move in game.moves)
            if move.cell in occupied and len(occupied) != 9:
                context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
                context.set_details("This cell is occupied")
                return game

            game.moves.append(move)

            winner = get_winner(game.moves)
            if winner is not None:
                game = ttt.Game(id=game_id, is_finished=True, winner=winner, moves=game.moves)
                self.games[game_id] = game
            else:
                game.turn = ttt.MARK_CROSS if game.turn == ttt.MARK_NOUGHT else ttt.MARK_NOUGHT
                if len(occupied) == 8:
                    game = ttt.Game(id=game_id, is_finished=True, moves=game.moves)
                    self.games[game_id] = game

            return game


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("port", help="Port of the Tic-Tac-Toe server.")
    args = parser.parse_args()

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    ttt_grpc.add_TicTacToeServicer_to_server(TicTacToeServicer(), server)
    server.add_insecure_port(f'[::]:{args.port}')
    server.start()
    print(f"Server listening on localhost:{args.port}...")
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print("^C received, shutting down server")
