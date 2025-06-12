import argparse
from stable_baselines3 import PPO
from stable_baselines3.common.env_util import make_vec_env
from aprendizaje.rl_env import UmbralEnv


def main() -> None:
    parser = argparse.ArgumentParser(description="Entrena la política de umbral")
    parser.add_argument(
        "--symbols",
        nargs="+",
        default=["BTC/EUR", "ETH/EUR", "ADA/EUR", "SOL/EUR", "BNB/EUR"],
        help="Símbolos a usar en el backtest",
    )
    parser.add_argument(
        "--timesteps",
        type=int,
        default=10,
        help="Número de pasos de entrenamiento",
    )
    parser.add_argument("--data-dir", default="datos", help="Directorio con las velas parquet")
    parser.add_argument(
        "--save-path",
        default="aprendizaje/umbral_rl.zip",
        help="Ruta donde guardar el modelo",
    )
    args = parser.parse_args()

    env = UmbralEnv(args.symbols, ruta_datos=args.data_dir)
    model = PPO("MlpPolicy", env, verbose=1)
    model.learn(total_timesteps=args.timesteps)
    model.save(args.save_path)
    print(f"Modelo RL guardado en {args.save_path}")


if __name__ == "__main__":
    main()