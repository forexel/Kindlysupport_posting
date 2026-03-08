import { useEffect, useState } from 'react';
import { Outlet, useNavigate, Link, useLocation } from 'react-router';
import { Button } from '../components/ui/button';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from '../components/ui/dropdown-menu';
import { MessageSquareQuote, BookOpen, Film, Send, Settings, LogOut, User } from 'lucide-react';
import { toast } from 'sonner';
import { api } from '../lib/api';

export function MainLayout() {
  const navigate = useNavigate();
  const location = useLocation();
  const [userEmail, setUserEmail] = useState(localStorage.getItem('userEmail') || 'admin@kindlysupport');

  useEffect(() => {
    let mounted = true;
    (async () => {
      try {
        await api('/api/config');
        if (mounted) {
          localStorage.setItem('auth', 'true');
          const fromStorage = localStorage.getItem('userEmail');
          if (fromStorage) setUserEmail(fromStorage);
        }
      } catch {
        localStorage.removeItem('auth');
        if (mounted) navigate('/login');
      }
    })();
    return () => {
      mounted = false;
    };
  }, [navigate]);

  const handleLogout = async () => {
    try {
      await api('/api/logout', 'POST', {});
    } catch {
      // ignore
    }
    localStorage.removeItem('auth');
    localStorage.removeItem('userEmail');
    toast.success('Вы вышли из системы');
    navigate('/login');
  };

  const navItems = [
    { path: '/phrases', label: 'Фразы', icon: MessageSquareQuote },
    { path: '/parables', label: 'Притчи', icon: BookOpen },
    { path: '/movies', label: 'Фильмы', icon: Film },
    { path: '/publications', label: 'Публикации', icon: Send },
    { path: '/settings', label: 'Настройки', icon: Settings },
  ];

  const isActive = (path: string) => location.pathname === path || location.pathname.startsWith(path + '/');

  return (
    <div className="min-h-screen bg-zinc-950">
      <header className="bg-zinc-900 border-b border-zinc-800 sticky top-0 z-50">
        <div className="container mx-auto px-4 h-16 flex items-center justify-between">
          <div className="flex items-center gap-8">
            <Link to="/phrases" className="font-bold text-xl text-zinc-50">KindlySupport</Link>
            <nav className="hidden md:flex items-center gap-1">
              {navItems.map((item) => {
                const Icon = item.icon;
                const active = isActive(item.path);
                return (
                  <Link
                    key={item.path}
                    to={item.path}
                    className={`flex items-center gap-2 px-4 py-2 rounded-md transition-colors ${
                      active ? 'bg-zinc-800 text-zinc-50' : 'text-zinc-400 hover:text-zinc-200 hover:bg-zinc-800/50'
                    }`}
                  >
                    <Icon className="h-4 w-4" />
                    {item.label}
                  </Link>
                );
              })}
            </nav>
          </div>
          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <Button variant="ghost" className="gap-2 text-zinc-400 hover:text-zinc-200">
                <User className="h-4 w-4" />
                <span className="hidden sm:inline">{userEmail}</span>
              </Button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="end" className="bg-zinc-900 border-zinc-800">
              <DropdownMenuLabel className="text-zinc-200">Мой аккаунт</DropdownMenuLabel>
              <DropdownMenuSeparator className="bg-zinc-800" />
              <DropdownMenuItem className="text-zinc-400 focus:text-zinc-200 focus:bg-zinc-800">{userEmail}</DropdownMenuItem>
              <DropdownMenuSeparator className="bg-zinc-800" />
              <DropdownMenuItem
                onClick={handleLogout}
                className="text-red-400 focus:text-red-300 focus:bg-zinc-800 cursor-pointer"
              >
                <LogOut className="mr-2 h-4 w-4" />
                Выйти
              </DropdownMenuItem>
            </DropdownMenuContent>
          </DropdownMenu>
        </div>
      </header>

      <main className="container mx-auto px-4 py-6">
        <Outlet />
      </main>
    </div>
  );
}
