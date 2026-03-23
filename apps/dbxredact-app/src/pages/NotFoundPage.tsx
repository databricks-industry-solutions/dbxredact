import { Link } from "react-router-dom";

export default function NotFoundPage() {
  return (
    <div className="flex flex-col items-center justify-center py-24 text-center">
      <h1 className="text-6xl font-bold text-gray-300 dark:text-gray-600 mb-4">404</h1>
      <p className="text-lg text-gray-600 dark:text-gray-400 mb-6">Page not found</p>
      <Link to="/" className="btn-primary">Back to Home</Link>
    </div>
  );
}
