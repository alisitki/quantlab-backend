/**
 * train.js: Orchestrates the training process.
 */
export async function trainModel(model, trainData, validData) {
  console.log(`Starting training...`);
  console.log(`Train set: ${trainData.X.length} rows`);
  console.log(`Valid set: ${validData.X.length} rows`);

  const startTime = Date.now();
  await model.train(trainData.X, trainData.y);
  const duration = (Date.now() - startTime) / 1000;

  console.log(`Training finished in ${duration.toFixed(2)}s`);

  // Simple validation log
  const validPreds = model.predict(validData.X);
  let correct = 0;
  for (let i = 0; i < validPreds.length; i++) {
    // Note: If model predicts shifted labels, we need to unshift here
    // But for now we assume labels are in same space or handled in model.predict
    if (validPreds[i] === validData.y[i]) {
      ++correct;
    }
  }

  const accuracy = correct / validPreds.length;
  console.log(`Validation Accuracy: ${(accuracy * 100).toFixed(2)}%`);

  return { accuracy, duration };
}
