"""
Train XGBoost model for AQI prediction.
"""

import pandas as pd
import numpy as np
import xgboost as xgb
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
import pickle
import os
from datetime import datetime
import argparse

from data_loader import AirQualityDataLoader


def calculate_metrics(y_true, y_pred):
    """Calculate evaluation metrics."""
    rmse = np.sqrt(mean_squared_error(y_true, y_pred))
    mae = mean_absolute_error(y_true, y_pred)
    
    # MAPE: Only calculate for non-zero values to avoid division issues
    mask = (y_true != 0) & (y_true > 0.1)  # Ignore values <= 0.1
    if mask.sum() > 0:
        mape = np.mean(np.abs((y_true[mask] - y_pred[mask]) / y_true[mask])) * 100
    else:
        mape = np.nan
    
    r2 = r2_score(y_true, y_pred)
    
    return {
        'rmse': rmse,
        'mae': mae,
        'mape': mape,
        'r2': r2
    }


def train_model(train_df: pd.DataFrame,
                val_df: pd.DataFrame,
                target_col: str = 'aqi_next'):
    """
    Train XGBoost model.
    
    Args:
        train_df: Training data
        val_df: Validation data
        target_col: Target column name
    
    Returns:
        Trained XGBoost model, metrics, feature_cols
    """
    # Feature columns (exclude target and metadata)
    exclude_cols = [
        target_col, 'aqi', 'datetime', 'location_id', 'location_name',
        'country', 'aqi_category', 'parameters', 'values',
        'year', 'month', 'day'  # Partition columns
    ]
    
    feature_cols = [col for col in train_df.columns if col not in exclude_cols]
    feature_cols = [col for col in feature_cols if col in train_df.columns]
    
    # Prepare data
    X_train = train_df[feature_cols].fillna(0)
    y_train = train_df[target_col].fillna(0)
    
    X_val = val_df[feature_cols].fillna(0)
    y_val = val_df[target_col].fillna(0)
    
    print(f"\nTraining features ({len(feature_cols)}):")
    for i, feat in enumerate(feature_cols[:15]):
        print(f"  {i+1}. {feat}")
    if len(feature_cols) > 15:
        print(f"  ... and {len(feature_cols) - 15} more")
    
    # XGBoost parameters (MAXIMIZED for high CPU capacity)
    # Aggressive settings to maximize performance:
    # - n_estimators: Increased to 5000 for extensive training
    # - max_depth: Increased to 8 with stronger regularization
    # - learning_rate: Kept moderate (0.03) for balance
    # - Early stopping: High patience (200) to allow full training
    params = {
        'objective': 'reg:squarederror',
        'eval_metric': 'rmse',
        'max_depth': 8,  # Increased (7→8) with stronger regularization to prevent overfitting
        'learning_rate': 0.03,  # Reduced (0.035→0.03) for better convergence with many iterations
        'subsample': 0.95,  # Increased (0.9→0.95) to use almost all data
        'colsample_bytree': 0.95,  # Increased (0.9→0.95) to use almost all features
        'colsample_bylevel': 0.95,  # Increased (0.9→0.95) for consistency
        'colsample_bynode': 0.95,  # Added for additional feature sampling
        'min_child_weight': 1,  # Reduced (2→1) for maximum flexibility
        'n_estimators': 5000,  # MAXIMIZED (3000→5000) for extensive training
        'random_state': 42,
        'n_jobs': -1,  # Use all CPU cores
        'early_stopping_rounds': 200,  # MAXIMIZED (150→200) for high patience
        'reg_alpha': 0.03,  # Further reduced (0.05→0.03) to allow maximum learning
        'reg_lambda': 0.2,  # Increased (0.15→0.2) for stronger L2 regularization
        'gamma': 0.15,  # Increased (0.1→0.15) for stronger regularization with deeper trees
        'tree_method': 'hist',  # Use histogram-based method for speed
        'max_bin': 512,  # Increased binning for finer splits
        'grow_policy': 'lossguide'  # Use loss-guided growth for better splits
    }
    
    # Train
    print(f"\nTraining XGBoost model...")
    print(f"Training samples: {len(X_train)}, Validation samples: {len(X_val)}")
    
    model = xgb.XGBRegressor(**params)
    
    # Fit with eval_set for early stopping
    model.fit(
        X_train, y_train,
        eval_set=[(X_val, y_val)],
        verbose=100
    )
    
    # Evaluate on validation set
    y_pred = model.predict(X_val)
    metrics = calculate_metrics(y_val, y_pred)
    
    print(f"\nValidation Metrics:")
    print(f"  RMSE: {metrics['rmse']:.2f}")
    print(f"  MAE:  {metrics['mae']:.2f}")
    if not np.isnan(metrics['mape']):
        print(f"  MAPE: {metrics['mape']:.2f}%")
    else:
        print(f"  MAPE: N/A (insufficient non-zero values)")
    print(f"  R²:   {metrics['r2']:.4f}")
    
    return model, metrics, feature_cols, params


def main():
    parser = argparse.ArgumentParser(description='Train XGBoost global model for AQI prediction')
    parser.add_argument('--start-date', type=str, default=None,
                       help='Start date (YYYY-MM-DD), default: None (all data)')
    parser.add_argument('--end-date', type=str, default=None,
                       help='End date (YYYY-MM-DD), default: None (all data)')
    parser.add_argument('--sample', type=float, default=None,
                       help='Sample fraction (for testing)')
    parser.add_argument('--output-dir', type=str, default='models',
                       help='Output directory for models')
    parser.add_argument('--target', type=str, default='aqi_next',
                       help='Target column: aqi_next (next hour) or aqi (current)')
    
    args = parser.parse_args()
    
    print("=" * 80)
    print("XGBoost AQI Prediction Training (Global Model)")
    print("=" * 80)
    
    # Load data
    loader = AirQualityDataLoader()
    df = loader.load_gold_layer(
        start_date=args.start_date,
        end_date=args.end_date,
        sample_frac=args.sample
    )
    
    # Create features
    print("\nCreating features...")
    df_features = loader.create_features(df)
    
    # Create target: next hour AQI
    if args.target == 'aqi_next':
        print("Creating target: AQI next hour...")
        df_features['aqi_next'] = df_features.groupby('location_id')['aqi'].shift(-1)
        df_features = df_features.dropna(subset=['aqi_next'])
        target_col = 'aqi_next'
    else:
        target_col = 'aqi'
    
    # Split data
    print("\nSplitting data...")
    train_df, val_df, test_df = loader.split_train_val_test(df_features)
    
    # Remove rows with NaN target
    train_df = train_df.dropna(subset=[target_col])
    val_df = val_df.dropna(subset=[target_col])
    
    # Train global model
    print("\nTraining global XGBoost model...")
    model, metrics, feature_cols, params = train_model(
        train_df, val_df, target_col=target_col
    )
    
    # Save model
    os.makedirs(args.output_dir, exist_ok=True)
    model_path = os.path.join(args.output_dir, 'xgboost_global.pkl')
    with open(model_path, 'wb') as f:
        pickle.dump({
            'model': model,
            'feature_cols': feature_cols,
            'metrics': metrics,
            'target_col': target_col,
            'train_date': datetime.now().isoformat(),
            'country_encoder': loader.country_encoder
        }, f)
    print(f"\n✓ Model saved to {model_path}")
    
    # Evaluate on test set
    print("\nEvaluating on test set...")
    X_test = test_df[feature_cols].fillna(0)
    y_test = test_df[target_col]
    y_pred_test = model.predict(X_test)
    test_metrics = calculate_metrics(y_test, y_pred_test)
    
    print(f"\nTest Set Metrics:")
    print(f"  RMSE: {test_metrics['rmse']:.2f}")
    print(f"  MAE:  {test_metrics['mae']:.2f}")
    if not np.isnan(test_metrics['mape']):
        print(f"  MAPE: {test_metrics['mape']:.2f}%")
    else:
        print(f"  MAPE: N/A (insufficient non-zero values)")
    print(f"  R²:   {test_metrics['r2']:.4f}")
    
    # Save evaluation results to file
    results_dir = os.path.join(args.output_dir, 'evaluation_results')
    os.makedirs(results_dir, exist_ok=True)
    results_file = os.path.join(results_dir, 'xgboost_evaluation.txt')
    
    with open(results_file, 'w') as f:
        f.write("=" * 80 + "\n")
        f.write("XGBoost Model Evaluation Results\n")
        f.write("=" * 80 + "\n\n")
        f.write(f"Training Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Target: {target_col}\n\n")
        
        f.write("Validation Set Metrics:\n")
        f.write("-" * 40 + "\n")
        f.write(f"RMSE: {metrics['rmse']:.4f}\n")
        f.write(f"MAE:  {metrics['mae']:.4f}\n")
        if not np.isnan(metrics['mape']):
            f.write(f"MAPE: {metrics['mape']:.4f}%\n")
        else:
            f.write(f"MAPE: N/A\n")
        f.write(f"R²:   {metrics['r2']:.4f}\n\n")
        
        f.write("Test Set Metrics:\n")
        f.write("-" * 40 + "\n")
        f.write(f"RMSE: {test_metrics['rmse']:.4f}\n")
        f.write(f"MAE:  {test_metrics['mae']:.4f}\n")
        if not np.isnan(test_metrics['mape']):
            f.write(f"MAPE: {test_metrics['mape']:.4f}%\n")
        else:
            f.write(f"MAPE: N/A\n")
        f.write(f"R²:   {test_metrics['r2']:.4f}\n\n")
        
        f.write("Data Split:\n")
        f.write("-" * 40 + "\n")
        f.write(f"Training samples: {len(train_df)}\n")
        f.write(f"Validation samples: {len(val_df)}\n")
        f.write(f"Test samples: {len(test_df)}\n\n")
        
        f.write("Model Parameters:\n")
        f.write("-" * 40 + "\n")
        for key, value in params.items():
            f.write(f"{key}: {value}\n")
        f.write(f"\nNumber of features: {len(feature_cols)}\n")
    
    print(f"\n✓ Evaluation results saved to {results_file}")
    
    print("\n" + "=" * 80)
    print("Training completed!")
    print("=" * 80)


if __name__ == "__main__":
    main()

