import { Link } from 'react-router';
import { Button } from '../components/ui/button';
import { Home } from 'lucide-react';

export function NotFound() {
  return (
    <div className="min-h-[60vh] flex items-center justify-center">
      <div className="text-center space-y-6">
        <div className="space-y-2">
          <h1 className="text-6xl font-bold text-zinc-50">404</h1>
          <h2 className="text-2xl font-semibold text-zinc-300">Страница не найдена</h2>
          <p className="text-zinc-400">Запрашиваемая страница не существует или была удалена.</p>
        </div>
        <Link to="/">
          <Button className="bg-blue-600 hover:bg-blue-700 text-white">
            <Home className="mr-2 h-4 w-4" />
            На главную
          </Button>
        </Link>
      </div>
    </div>
  );
}
